package azureblob

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/argoproj/argo-workflows/v3/errors"
	"github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	waitutil "github.com/argoproj/argo-workflows/v3/util/wait"
	"github.com/argoproj/argo-workflows/v3/workflow/artifacts/common"
	executorretry "github.com/argoproj/argo-workflows/v3/workflow/executor/retry"
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/util/retry"
)

// ArtifactDriver is a driver for Azure Blob Storage. Based on Microsoft go client: https://github.com/Azure/azure-sdk-for-go/tree/main/sdk/storage/azblob#readme
type ArtifactDriver struct {
	AccountName string
	SharedKey   string
}

var _ common.ArtifactDriver = &ArtifactDriver{}

// ValidateArtifact validates AzureBlob artifact
func ValidateArtifact(errPrefix string, art *wfv1.AzureBlobArtifact) error {
	if art.Container == "" {
		return errors.Errorf(errors.CodeBadRequest, "%s.container is required", errPrefix)
	}
	if art.Name == "" {
		return errors.Errorf(errors.CodeBadRequest, "%s.name is required", errPrefix)
	}

	hasAccountName := art.AccountNameSecret == nil
	hasSharedKey := art.SecretKeySecret == nil

	if hasSharedKey {
		return errors.Errorf(errors.CodeBadRequest, "%s.SecretKeySecret is required", errPrefix)
	}
	if hasAccountName {
		return errors.Errorf(errors.CodeBadRequest, "%s.AccountNameSecret is required", errPrefix)
	}

	return nil
}

func isTransientAzureBlobErr(err error) bool {
	if err == io.ErrUnexpectedEOF {
		return true
	}
	switch e := err.(type) {

	case *azblob.StorageError:
		return e.ErrorCode == azblob.StorageErrorCodeInternalError || e.ErrorCode == azblob.StorageErrorCodeOperationTimedOut || e.ErrorCode == azblob.StorageErrorCodePendingCopyOperation
	case *url.Error:
		// Retry socket-level errors ECONNREFUSED and ENETUNREACH (from syscall).
		// Unfortunately the error type is unexported, so we resort to string
		// matching.
		retriable := []string{"connection refused", "connection reset"}
		for _, s := range retriable {
			if strings.Contains(e.Error(), s) {
				return true
			}
		}
	case interface{ Temporary() bool }:
		if e.Temporary() {
			return true
		}
	}
	if e, ok := err.(interface{ Unwrap() error }); ok {
		return isTransientAzureBlobErr(e.Unwrap())
	}
	return false
}

// newAzureBlobClient instantiates a new azure blob client object.
func (azblobDriver *ArtifactDriver) newAzureBlobClient(ctx context.Context, containerName string) (*azblob.ContainerClient, error) {

	cred, err := azblob.NewSharedKeyCredential(azblobDriver.AccountName, azblobDriver.SharedKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
		return nil, err
	}

	serviceClient, err := azblob.NewServiceClientWithSharedKey(fmt.Sprintf("https://%s.blob.core.windows.net/", azblobDriver.AccountName), cred, nil)

	if err != nil {
		log.Fatal("There was an unexpected error creating Azure service client: " + err.Error())
		return nil, err
	}

	pager := serviceClient.ListContainers(&azblob.ListContainersOptions{Prefix: &containerName})

	exists := false

	for pager.NextPage(ctx) {
		resp := pager.PageResponse()
		for _, v := range resp.ContainerItems {
			if *v.Name == containerName {
				exists = true
				break
			}
		}

		if exists {
			break
		}
	}

	log.Debug(azblobDriver.AccountName)
	log.Debug(containerName)

	containerClient, err := serviceClient.NewContainerClient(containerName)

	if err != nil {
		log.Fatal("There was an unexpected error creating Azure container client: " + err.Error())
		return nil, err
	}

	return containerClient, nil
}

// Load downloads artifacts from Azure Blob Storage
func (azblobDriver *ArtifactDriver) Load(inputArtifact *wfv1.Artifact, path string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := waitutil.Backoff(executorretry.ExecutorRetry,
		func() (bool, error) {
			log.Infof("Azure Blob Load path: %s, key: %s", path, inputArtifact.AzureBlob.Name)

			azureBlobClient, err := azblobDriver.newAzureBlobClient(ctx, inputArtifact.AzureBlob.Container)
			if err != nil {
				return !isTransientAzureBlobErr(err), fmt.Errorf("failed to create new Azure Blob client: %v", err)
			}

			blobClient, err := azureBlobClient.NewBlobClient(inputArtifact.AzureBlob.Name)
			if err != nil {
				return !isTransientAzureBlobErr(err), fmt.Errorf("failed to create new Blob client: %v", err)
			}

			downloadResponse, err := blobClient.Download(ctx, nil)

			if err != nil {
				return !isTransientAzureBlobErr(err), fmt.Errorf("failed to download file from path: %s %s", inputArtifact.AzureBlob.Container, inputArtifact.AzureBlob.Name)
			}

			// RetryReaderOptions has a lot of in-depth tuning abilities, but for the sake of simplicity, we'll omit those here.
			reader := downloadResponse.Body(&azblob.RetryReaderOptions{MaxRetryRequests: 20})
			tmp, err := os.Create(path)

			if err != nil {
				return !isTransientAzureBlobErr(err), fmt.Errorf("failed to create file in path: %s", path)
			}

			_, err = io.Copy(tmp, reader)

			if err != nil {
				return !isTransientAzureBlobErr(err), fmt.Errorf("failed to copy file in path: %s", path)
			}

			tmp.Sync()

			return true, nil
		})

	return err
}

// Save  artifacts to Azure Blob Storage
func (azblobDriver *ArtifactDriver) Save(path string, outputArtifact *wfv1.Artifact) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := waitutil.Backoff(executorretry.ExecutorRetry,
		func() (bool, error) {
			log.Infof("Trying to create blob %s inside container %s", path, outputArtifact.AzureBlob.Container)

			azureBlobClient, err := azblobDriver.newAzureBlobClient(ctx, outputArtifact.AzureBlob.Container)

			if err != nil {
				return !isTransientAzureBlobErr(err), fmt.Errorf("Failed to create new azure blob client: %v", err)
			}

			blobClient, err := azureBlobClient.NewBlockBlobClient(outputArtifact.AzureBlob.Name)

			if err != nil {
				return !isTransientAzureBlobErr(err), fmt.Errorf("Failed to create new azure block blob client: %v", err)
			}

			file, err := os.Open(path)

			if err != nil {
				return !isTransientAzureBlobErr(err), fmt.Errorf("Failed to open file path: %s. ERROR: %v", path, err)
			}

			defer file.Close()

			response, err := blobClient.UploadFile(ctx, file, azblob.UploadOption{
				BlockSize:   4 * 1024 * 1024,
				Parallelism: 16})

			if response.StatusCode != 201 || err != nil {
				return !isTransientAzureBlobErr(err), fmt.Errorf("Failed to upload file: %s. STATUS CODE: %v .ERROR: %w", path, response.Status, err)
			}

			return true, nil
		})

	return err

}

func (azblobDriver *ArtifactDriver) Delete(artifact *wfv1.Artifact) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := retry.OnError(retry.DefaultBackoff, isTransientAzureBlobErr, func() error {
		log.Infof("Azure Blob Delete artifact: name: %s", artifact.AzureBlob.Name)

		azureBlobClient, err := azblobDriver.newAzureBlobClient(ctx, artifact.AzureBlob.Container)
		if err != nil {
			return err
		}

		blobClient, err := azureBlobClient.NewBlockBlobClient(artifact.AzureBlob.Name)

		if err != nil {
			return errors.InternalWrapError(err)
		}

		response, err := blobClient.Delete(ctx, &azblob.BlobDeleteOptions{})

		if response.RawResponse.StatusCode != 202 {
			log.Warn("The artifact was not deleted.")
		}

		if err != nil {
			return err
		}

		return nil
	})

	return err
}

func (azblobDriver *ArtifactDriver) ListObjects(artifact *wfv1.Artifact) ([]string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	azureBlobClient, err := azblobDriver.newAzureBlobClient(ctx, artifact.AzureBlob.Container)

	if err != nil {
		return nil, errors.InternalWrapError(err)
	}

	pager := azureBlobClient.ListBlobsFlat(nil)

	var objectsListed []string

	for pager.NextPage(ctx) {
		resp := pager.PageResponse()

		for _, blob := range resp.ListBlobsFlatSegmentResponse.Segment.BlobItems {
			objectsListed = append(objectsListed, *blob.Name)
		}
	}

	if err = pager.Err(); err != nil {
		return nil, errors.InternalWrapError(err)
	}

	return objectsListed, nil
}

func (azblobDriver *ArtifactDriver) IsDirectory(artifact *wfv1.Artifact) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	azureBlobClient, err := azblobDriver.newAzureBlobClient(ctx, artifact.AzureBlob.Container)
	if err != nil {
		return false, errors.InternalWrapError(err)
	}

	blobClient, err := azureBlobClient.NewBlockBlobClient(artifact.AzureBlob.Name)

	prop, err := blobClient.GetProperties(ctx, &azblob.BlobGetPropertiesOptions{})

	if err != nil {
		return false, errors.InternalWrapError(err)
	}

	return prop.Metadata["Hdi_isfolder"] == "true", nil
}

func (azblobDriver *ArtifactDriver) OpenStream(inputArtifact *v1alpha1.Artifact) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	azureBlobClient, err := azblobDriver.newAzureBlobClient(ctx, inputArtifact.AzureBlob.Container)
	if err != nil {
		return nil, errors.InternalWrapError(err)
	}

	blobClient, err := azureBlobClient.NewBlobClient(inputArtifact.AzureBlob.Name)
	if err != nil {
		return nil, errors.InternalWrapError(err)
	}

	downloadResponse, err := blobClient.Download(ctx, nil)

	if err != nil {
		return nil, errors.InternalWrapError(err)
	}

	// RetryReaderOptions has a lot of in-depth tuning abilities, but for the sake of simplicity, we'll omit those here.
	reader := downloadResponse.Body(&azblob.RetryReaderOptions{MaxRetryRequests: 20})

	return reader, nil
}
