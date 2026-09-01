package gistgooglecloudstorageclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"

	"github.com/wieoapps/gist"
	gistproto "github.com/wieoapps/gist/proto"
	"github.com/wieoapps/gist/internal/rpcconn"
)

type Service struct {
	server    *gist.Server
	serviceID string
}

func NewService(server *gist.Server, serviceID string) *Service {
	return &Service{server: server, serviceID: serviceID}
}

func init() {
	gist.RegisterServiceType(NewService)
}

func (s *Service) Exists(ctx context.Context, bucketID, fileName string) (bool, error) {
	resp, err := rpcconn.MustFor(s.server).GoogleCloudStorage.Exists(ctx, &gistproto.GoogleCloudStorageExistsRequest{
		ServiceId: s.serviceID,
		BucketId:  bucketID,
		FileName:  fileName,
	})
	if err != nil {
		return false, fmt.Errorf("gist-google-cloud-storage-client: exists: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, fmt.Errorf("gist-google-cloud-storage-client: exists: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetExists(), nil
}

func (s *Service) Store(ctx context.Context, bucketID, path string, fileHeader *multipart.FileHeader) (*string, *string, error) {
	if fileHeader == nil || fileHeader.Size == 0 || len(fileHeader.Filename) == 0 {
		return nil, nil, fmt.Errorf("file or filename is empty")
	}

	file, err := fileHeader.Open()
	if err != nil {
		return nil, nil, fmt.Errorf("error opening file for reading: %v", err)
	}
	defer func(file multipart.File) {
		_ = file.Close()
	}(file)

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading file: %v", err)
	}

	resp, err := rpcconn.MustFor(s.server).GoogleCloudStorage.Store(ctx, &gistproto.GoogleCloudStorageStoreRequest{
		ServiceId:   s.serviceID,
		BucketId:    bucketID,
		Path:        path,
		FileName:    fileHeader.Filename,
		FileBytes:   fileBytes,
		ContentType: fileHeader.Header.Get("Content-Type"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("gist-google-cloud-storage-client: store: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return nil, nil, fmt.Errorf("gist-google-cloud-storage-client: store: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	respPath, respFileName := resp.GetPath(), resp.GetFileName()
	return &respPath, &respFileName, nil
}

// CreateBucket creates a new bucket - see gist-server's CreateBucket for
// the underlying REST call and its 409-is-not-an-error convention;
// created reports whether this call was the one that created it.
func (s *Service) CreateBucket(ctx context.Context, bucketID, location string) (created bool, err error) {
	resp, err := rpcconn.MustFor(s.server).GoogleCloudStorage.CreateBucket(ctx, &gistproto.GoogleCloudStorageCreateBucketRequest{
		ServiceId: s.serviceID,
		BucketId:  bucketID,
		Location:  location,
	})
	if err != nil {
		return false, fmt.Errorf("gist-google-cloud-storage-client: create bucket: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, fmt.Errorf("gist-google-cloud-storage-client: create bucket: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetCreated(), nil
}

// GetBucket retrieves the bucket's metadata, decoding it into out (a
// pointer, same convention json.Unmarshal uses). found reports whether
// the bucket existed - out is left untouched when it didn't.
func (s *Service) GetBucket(ctx context.Context, bucketID string, out any) (found bool, err error) {
	resp, err := rpcconn.MustFor(s.server).GoogleCloudStorage.GetBucket(ctx, &gistproto.GoogleCloudStorageGetBucketRequest{
		ServiceId: s.serviceID,
		BucketId:  bucketID,
	})
	if err != nil {
		return false, fmt.Errorf("gist-google-cloud-storage-client: get bucket: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, fmt.Errorf("gist-google-cloud-storage-client: get bucket: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	if !resp.GetFound() {
		return false, nil
	}
	if err := json.Unmarshal(resp.GetAttrsJson(), out); err != nil {
		return false, fmt.Errorf("gist-google-cloud-storage-client: could not decode bucket attrs: %w", err)
	}
	return true, nil
}

// UpdateBucket merges attrs' fields into the bucket's metadata - e.g.
// map[string]any{"labels": map[string]string{"team": "platform"}}.
// found reports whether the bucket existed to update.
func (s *Service) UpdateBucket(ctx context.Context, bucketID string, attrs any) (found bool, err error) {
	attrsJSON, err := json.Marshal(attrs)
	if err != nil {
		return false, fmt.Errorf("gist-google-cloud-storage-client: could not encode bucket attrs: %w", err)
	}

	resp, err := rpcconn.MustFor(s.server).GoogleCloudStorage.UpdateBucket(ctx, &gistproto.GoogleCloudStorageUpdateBucketRequest{
		ServiceId: s.serviceID,
		BucketId:  bucketID,
		AttrsJson: attrsJSON,
	})
	if err != nil {
		return false, fmt.Errorf("gist-google-cloud-storage-client: update bucket: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, fmt.Errorf("gist-google-cloud-storage-client: update bucket: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetFound(), nil
}

// DeleteBucket deletes the bucket - Cloud Storage requires it to be
// empty first (see gist-server's DeleteBucket). found reports whether
// the bucket existed to delete.
func (s *Service) DeleteBucket(ctx context.Context, bucketID string) (found bool, err error) {
	resp, err := rpcconn.MustFor(s.server).GoogleCloudStorage.DeleteBucket(ctx, &gistproto.GoogleCloudStorageDeleteBucketRequest{
		ServiceId: s.serviceID,
		BucketId:  bucketID,
	})
	if err != nil {
		return false, fmt.Errorf("gist-google-cloud-storage-client: delete bucket: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, fmt.Errorf("gist-google-cloud-storage-client: delete bucket: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetFound(), nil
}

// GetObject downloads fileName's raw content from bucketID. found
// reports whether the object existed.
func (s *Service) GetObject(ctx context.Context, bucketID, fileName string) (found bool, content []byte, err error) {
	resp, err := rpcconn.MustFor(s.server).GoogleCloudStorage.GetObject(ctx, &gistproto.GoogleCloudStorageGetObjectRequest{
		ServiceId: s.serviceID,
		BucketId:  bucketID,
		FileName:  fileName,
	})
	if err != nil {
		return false, nil, fmt.Errorf("gist-google-cloud-storage-client: get object: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, nil, fmt.Errorf("gist-google-cloud-storage-client: get object: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	if !resp.GetFound() {
		return false, nil, nil
	}
	return true, resp.GetContent(), nil
}

// UpdateObjectMetadata merges metadata into fileName's custom key/value
// metadata. found reports whether the object existed to update; updated
// is its full custom metadata after the merge.
func (s *Service) UpdateObjectMetadata(ctx context.Context, bucketID, fileName string, metadata map[string]string) (found bool, updated map[string]string, err error) {
	resp, err := rpcconn.MustFor(s.server).GoogleCloudStorage.UpdateObjectMetadata(ctx, &gistproto.GoogleCloudStorageUpdateObjectMetadataRequest{
		ServiceId: s.serviceID,
		BucketId:  bucketID,
		FileName:  fileName,
		Metadata:  metadata,
	})
	if err != nil {
		return false, nil, fmt.Errorf("gist-google-cloud-storage-client: update object metadata: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, nil, fmt.Errorf("gist-google-cloud-storage-client: update object metadata: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	if !resp.GetFound() {
		return false, nil, nil
	}
	return true, resp.GetMetadata(), nil
}

// DeleteObject deletes fileName from bucketID. found reports whether the
// object existed to delete.
func (s *Service) DeleteObject(ctx context.Context, bucketID, fileName string) (found bool, err error) {
	resp, err := rpcconn.MustFor(s.server).GoogleCloudStorage.DeleteObject(ctx, &gistproto.GoogleCloudStorageDeleteObjectRequest{
		ServiceId: s.serviceID,
		BucketId:  bucketID,
		FileName:  fileName,
	})
	if err != nil {
		return false, fmt.Errorf("gist-google-cloud-storage-client: delete object: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, fmt.Errorf("gist-google-cloud-storage-client: delete object: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetFound(), nil
}
