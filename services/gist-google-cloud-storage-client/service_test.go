package gistgooglecloudstorageclient

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"testing"

	"google.golang.org/grpc"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist-proto"
	"github.com/wieoapps/gist/internal/rpcconn"
)

// fakeGoogleCloudStorageClient records the last request it received (Exists or Store)
// and returns a scripted response.
type fakeGoogleCloudStorageClient struct {
	lastExistsReq *gistproto.GoogleCloudStorageExistsRequest
	existsResp    *gistproto.GoogleCloudStorageExistsResponse
	existsErr     error

	lastStoreReq *gistproto.GoogleCloudStorageStoreRequest
	storeResp    *gistproto.GoogleCloudStorageStoreResponse
	storeErr     error

	lastCreateBucketReq *gistproto.GoogleCloudStorageCreateBucketRequest
	createBucketResp    *gistproto.GoogleCloudStorageCreateBucketResponse
	createBucketErr     error

	lastGetBucketReq *gistproto.GoogleCloudStorageGetBucketRequest
	getBucketResp    *gistproto.GoogleCloudStorageGetBucketResponse
	getBucketErr     error

	lastUpdateBucketReq *gistproto.GoogleCloudStorageUpdateBucketRequest
	updateBucketResp    *gistproto.GoogleCloudStorageUpdateBucketResponse
	updateBucketErr     error

	lastDeleteBucketReq *gistproto.GoogleCloudStorageDeleteBucketRequest
	deleteBucketResp    *gistproto.GoogleCloudStorageDeleteBucketResponse
	deleteBucketErr     error

	lastGetObjectReq *gistproto.GoogleCloudStorageGetObjectRequest
	getObjectResp    *gistproto.GoogleCloudStorageGetObjectResponse
	getObjectErr     error

	lastUpdateObjectMetadataReq *gistproto.GoogleCloudStorageUpdateObjectMetadataRequest
	updateObjectMetadataResp    *gistproto.GoogleCloudStorageUpdateObjectMetadataResponse
	updateObjectMetadataErr     error

	lastDeleteObjectReq *gistproto.GoogleCloudStorageDeleteObjectRequest
	deleteObjectResp    *gistproto.GoogleCloudStorageDeleteObjectResponse
	deleteObjectErr     error
}

func (f *fakeGoogleCloudStorageClient) Exists(_ context.Context, in *gistproto.GoogleCloudStorageExistsRequest, _ ...grpc.CallOption) (*gistproto.GoogleCloudStorageExistsResponse, error) {
	f.lastExistsReq = in
	if f.existsErr != nil {
		return nil, f.existsErr
	}
	return f.existsResp, nil
}

func (f *fakeGoogleCloudStorageClient) Store(_ context.Context, in *gistproto.GoogleCloudStorageStoreRequest, _ ...grpc.CallOption) (*gistproto.GoogleCloudStorageStoreResponse, error) {
	f.lastStoreReq = in
	if f.storeErr != nil {
		return nil, f.storeErr
	}
	return f.storeResp, nil
}

func (f *fakeGoogleCloudStorageClient) CreateBucket(_ context.Context, in *gistproto.GoogleCloudStorageCreateBucketRequest, _ ...grpc.CallOption) (*gistproto.GoogleCloudStorageCreateBucketResponse, error) {
	f.lastCreateBucketReq = in
	if f.createBucketErr != nil {
		return nil, f.createBucketErr
	}
	return f.createBucketResp, nil
}

func (f *fakeGoogleCloudStorageClient) GetBucket(_ context.Context, in *gistproto.GoogleCloudStorageGetBucketRequest, _ ...grpc.CallOption) (*gistproto.GoogleCloudStorageGetBucketResponse, error) {
	f.lastGetBucketReq = in
	if f.getBucketErr != nil {
		return nil, f.getBucketErr
	}
	return f.getBucketResp, nil
}

func (f *fakeGoogleCloudStorageClient) UpdateBucket(_ context.Context, in *gistproto.GoogleCloudStorageUpdateBucketRequest, _ ...grpc.CallOption) (*gistproto.GoogleCloudStorageUpdateBucketResponse, error) {
	f.lastUpdateBucketReq = in
	if f.updateBucketErr != nil {
		return nil, f.updateBucketErr
	}
	return f.updateBucketResp, nil
}

func (f *fakeGoogleCloudStorageClient) DeleteBucket(_ context.Context, in *gistproto.GoogleCloudStorageDeleteBucketRequest, _ ...grpc.CallOption) (*gistproto.GoogleCloudStorageDeleteBucketResponse, error) {
	f.lastDeleteBucketReq = in
	if f.deleteBucketErr != nil {
		return nil, f.deleteBucketErr
	}
	return f.deleteBucketResp, nil
}

func (f *fakeGoogleCloudStorageClient) GetObject(_ context.Context, in *gistproto.GoogleCloudStorageGetObjectRequest, _ ...grpc.CallOption) (*gistproto.GoogleCloudStorageGetObjectResponse, error) {
	f.lastGetObjectReq = in
	if f.getObjectErr != nil {
		return nil, f.getObjectErr
	}
	return f.getObjectResp, nil
}

func (f *fakeGoogleCloudStorageClient) UpdateObjectMetadata(_ context.Context, in *gistproto.GoogleCloudStorageUpdateObjectMetadataRequest, _ ...grpc.CallOption) (*gistproto.GoogleCloudStorageUpdateObjectMetadataResponse, error) {
	f.lastUpdateObjectMetadataReq = in
	if f.updateObjectMetadataErr != nil {
		return nil, f.updateObjectMetadataErr
	}
	return f.updateObjectMetadataResp, nil
}

func (f *fakeGoogleCloudStorageClient) DeleteObject(_ context.Context, in *gistproto.GoogleCloudStorageDeleteObjectRequest, _ ...grpc.CallOption) (*gistproto.GoogleCloudStorageDeleteObjectResponse, error) {
	f.lastDeleteObjectReq = in
	if f.deleteObjectErr != nil {
		return nil, f.deleteObjectErr
	}
	return f.deleteObjectResp, nil
}

// buildFileHeader constructs a real *multipart.FileHeader by writing a
// form file into a multipart body and re-parsing it - the standard way
// to get one without an actual HTTP request, since multipart.FileHeader
// has no public constructor.
func buildFileHeader(t *testing.T, fieldName, fileName string, content []byte) *multipart.FileHeader {
	t.Helper()

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormFile(fieldName, fileName)
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatalf("write form file content: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	r := multipart.NewReader(&buf, w.Boundary())
	form, err := r.ReadForm(int64(len(content)) + 1024)
	if err != nil {
		t.Fatalf("ReadForm: %v", err)
	}
	files := form.File[fieldName]
	if len(files) != 1 {
		t.Fatalf("expected 1 file under field %q, got %d", fieldName, len(files))
	}
	return files[0]
}

func TestExists_SendsRequestAndDecodesResult(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{existsResp: &gistproto.GoogleCloudStorageExistsResponse{Exists: true}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	exists, err := svc.Exists(context.Background(), "my-bucket", "file.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Fatal("expected Exists to return true")
	}
	if fake.lastExistsReq.GetServiceId() != "gcs1" || fake.lastExistsReq.GetBucketId() != "my-bucket" || fake.lastExistsReq.GetFileName() != "file.txt" {
		t.Fatalf("unexpected request: %+v", fake.lastExistsReq)
	}
}

func TestExists_WireErrorBecomesError(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{existsResp: &gistproto.GoogleCloudStorageExistsResponse{ErrorCode: "internal", ErrorMessage: "bucket not found"}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	_, err := svc.Exists(context.Background(), "my-bucket", "file.txt")
	if err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

func TestExists_TransportErrorPropagates(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{existsErr: context.DeadlineExceeded}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	_, err := svc.Exists(context.Background(), "my-bucket", "file.txt")
	if err == nil {
		t.Fatal("expected an error when the RPC call itself fails")
	}
}

func TestStore_SendsFileBytesAndFilename(t *testing.T) {
	fh := buildFileHeader(t, "file", "photo.png", []byte("fake-image-bytes"))

	fake := &fakeGoogleCloudStorageClient{storeResp: &gistproto.GoogleCloudStorageStoreResponse{Path: "uploads/", FileName: "photo.png"}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	path, fileName, err := svc.Store(context.Background(), "my-bucket", "uploads/", fh)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if path == nil || *path != "uploads/" {
		t.Fatalf("unexpected returned path: %v", path)
	}
	if fileName == nil || *fileName != "photo.png" {
		t.Fatalf("unexpected returned file name: %v", fileName)
	}

	if fake.lastStoreReq.GetServiceId() != "gcs1" || fake.lastStoreReq.GetBucketId() != "my-bucket" || fake.lastStoreReq.GetPath() != "uploads/" {
		t.Fatalf("unexpected request: %+v", fake.lastStoreReq)
	}
	if fake.lastStoreReq.GetFileName() != "photo.png" {
		t.Fatalf("expected filename photo.png, got %q", fake.lastStoreReq.GetFileName())
	}
	if string(fake.lastStoreReq.GetFileBytes()) != "fake-image-bytes" {
		t.Fatalf("expected file bytes to be sent verbatim, got %q", fake.lastStoreReq.GetFileBytes())
	}
	// multipart.Writer.CreateFormFile always sets this on the part -
	// confirms fileHeader.Header's Content-Type makes it onto the wire.
	if fake.lastStoreReq.GetContentType() != "application/octet-stream" {
		t.Errorf("ContentType = %q, want application/octet-stream (from the multipart part header)", fake.lastStoreReq.GetContentType())
	}
}

func TestStore_NilFileHeaderReturnsError(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	_, _, err := svc.Store(context.Background(), "my-bucket", "uploads/", nil)
	if err == nil {
		t.Fatal("expected an error for a nil file header")
	}
	if fake.lastStoreReq != nil {
		t.Fatal("expected the RPC to never be called for a nil file header")
	}
}

func TestStore_EmptyFileReturnsError(t *testing.T) {
	fh := buildFileHeader(t, "file", "empty.txt", []byte{})
	fake := &fakeGoogleCloudStorageClient{}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	_, _, err := svc.Store(context.Background(), "my-bucket", "uploads/", fh)
	if err == nil {
		t.Fatal("expected an error for a zero-size file (matches fileHeader.Size == 0 guard)")
	}
}

func TestStore_WireErrorBecomesError(t *testing.T) {
	fh := buildFileHeader(t, "file", "photo.png", []byte("data"))
	fake := &fakeGoogleCloudStorageClient{storeResp: &gistproto.GoogleCloudStorageStoreResponse{ErrorCode: "internal", ErrorMessage: "write failed"}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	_, _, err := svc.Store(context.Background(), "my-bucket", "uploads/", fh)
	if err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

func TestStore_TransportErrorPropagates(t *testing.T) {
	fh := buildFileHeader(t, "file", "photo.png", []byte("data"))
	fake := &fakeGoogleCloudStorageClient{storeErr: context.DeadlineExceeded}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	_, _, err := svc.Store(context.Background(), "my-bucket", "uploads/", fh)
	if err == nil {
		t.Fatal("expected an error when the RPC call itself fails")
	}
}

func TestCreateBucket_SendsRequestAndDecodesResult(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{createBucketResp: &gistproto.GoogleCloudStorageCreateBucketResponse{Created: true}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	created, err := svc.CreateBucket(context.Background(), "my-bucket", "US")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !created {
		t.Error("expected created=true")
	}
	if fake.lastCreateBucketReq.GetBucketId() != "my-bucket" || fake.lastCreateBucketReq.GetLocation() != "US" {
		t.Fatalf("unexpected request: %+v", fake.lastCreateBucketReq)
	}
}

func TestCreateBucket_WireErrorBecomesError(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{createBucketResp: &gistproto.GoogleCloudStorageCreateBucketResponse{ErrorCode: "internal", ErrorMessage: "boom"}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	if _, err := svc.CreateBucket(context.Background(), "my-bucket", "US"); err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

type bucketAttrs struct {
	Name     string `json:"name"`
	Location string `json:"location"`
}

func TestGetBucket_FoundDecodesIntoOut(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{getBucketResp: &gistproto.GoogleCloudStorageGetBucketResponse{Found: true, AttrsJson: []byte(`{"name":"real-bucket","location":"US"}`)}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	var out bucketAttrs
	found, err := svc.GetBucket(context.Background(), "my-bucket", &out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if out != (bucketAttrs{Name: "real-bucket", Location: "US"}) {
		t.Errorf("out = %+v", out)
	}
}

func TestGetBucket_NotFoundLeavesOutUntouched(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{getBucketResp: &gistproto.GoogleCloudStorageGetBucketResponse{Found: false}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	out := bucketAttrs{Name: "unchanged"}
	found, err := svc.GetBucket(context.Background(), "my-bucket", &out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("expected found=false")
	}
	if out.Name != "unchanged" {
		t.Errorf("expected out to be left untouched, got %+v", out)
	}
}

func TestUpdateBucket_EncodesAttrsAndSendsRequest(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{updateBucketResp: &gistproto.GoogleCloudStorageUpdateBucketResponse{Found: true}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	found, err := svc.UpdateBucket(context.Background(), "my-bucket", map[string]any{"labels": map[string]string{"team": "platform"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Error("expected found=true")
	}
	var decoded map[string]any
	if err := json.Unmarshal(fake.lastUpdateBucketReq.GetAttrsJson(), &decoded); err != nil {
		t.Fatalf("could not decode sent attrs: %v", err)
	}
	if _, ok := decoded["labels"]; !ok {
		t.Errorf("decoded = %v, want a labels field", decoded)
	}
}

func TestUpdateBucket_NotFound(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{updateBucketResp: &gistproto.GoogleCloudStorageUpdateBucketResponse{Found: false}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	found, err := svc.UpdateBucket(context.Background(), "my-bucket", map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("expected found=false")
	}
}

func TestDeleteBucket_SendsRequestAndReportsFound(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{deleteBucketResp: &gistproto.GoogleCloudStorageDeleteBucketResponse{Found: true}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	found, err := svc.DeleteBucket(context.Background(), "my-bucket")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Error("expected found=true")
	}
	if fake.lastDeleteBucketReq.GetBucketId() != "my-bucket" {
		t.Errorf("unexpected request: %+v", fake.lastDeleteBucketReq)
	}
}

func TestDeleteBucket_WireErrorBecomesError(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{deleteBucketResp: &gistproto.GoogleCloudStorageDeleteBucketResponse{ErrorCode: "internal", ErrorMessage: "not empty"}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	if _, err := svc.DeleteBucket(context.Background(), "my-bucket"); err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

func TestGetObject_FoundReturnsContent(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{getObjectResp: &gistproto.GoogleCloudStorageGetObjectResponse{Found: true, Content: []byte("hello world")}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	found, content, err := svc.GetObject(context.Background(), "my-bucket", "report.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if string(content) != "hello world" {
		t.Errorf("content = %q, want hello world", content)
	}
	if fake.lastGetObjectReq.GetBucketId() != "my-bucket" || fake.lastGetObjectReq.GetFileName() != "report.txt" {
		t.Fatalf("unexpected request: %+v", fake.lastGetObjectReq)
	}
}

func TestGetObject_NotFoundReturnsNilContent(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{getObjectResp: &gistproto.GoogleCloudStorageGetObjectResponse{Found: false}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	found, content, err := svc.GetObject(context.Background(), "my-bucket", "missing.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found || content != nil {
		t.Errorf("expected found=false content=nil, got found=%v content=%s", found, content)
	}
}

func TestUpdateObjectMetadata_SendsRequestAndReturnsMetadata(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{updateObjectMetadataResp: &gistproto.GoogleCloudStorageUpdateObjectMetadataResponse{Found: true, Metadata: map[string]string{"custom-key": "custom-value"}}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	found, metadata, err := svc.UpdateObjectMetadata(context.Background(), "my-bucket", "report.txt", map[string]string{"custom-key": "custom-value"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Error("expected found=true")
	}
	if metadata["custom-key"] != "custom-value" {
		t.Errorf("metadata = %v", metadata)
	}
	if fake.lastUpdateObjectMetadataReq.GetMetadata()["custom-key"] != "custom-value" {
		t.Fatalf("unexpected request: %+v", fake.lastUpdateObjectMetadataReq)
	}
}

func TestUpdateObjectMetadata_NotFound(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{updateObjectMetadataResp: &gistproto.GoogleCloudStorageUpdateObjectMetadataResponse{Found: false}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	found, metadata, err := svc.UpdateObjectMetadata(context.Background(), "my-bucket", "missing.txt", map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found || metadata != nil {
		t.Errorf("expected found=false metadata=nil, got found=%v metadata=%v", found, metadata)
	}
}

func TestDeleteObject_SendsRequestAndReportsFound(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{deleteObjectResp: &gistproto.GoogleCloudStorageDeleteObjectResponse{Found: true}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	found, err := svc.DeleteObject(context.Background(), "my-bucket", "report.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Error("expected found=true")
	}
	if fake.lastDeleteObjectReq.GetBucketId() != "my-bucket" || fake.lastDeleteObjectReq.GetFileName() != "report.txt" {
		t.Fatalf("unexpected request: %+v", fake.lastDeleteObjectReq)
	}
}

func TestDeleteObject_WireErrorBecomesError(t *testing.T) {
	fake := &fakeGoogleCloudStorageClient{deleteObjectResp: &gistproto.GoogleCloudStorageDeleteObjectResponse{ErrorCode: "internal", ErrorMessage: "boom"}}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{GoogleCloudStorage: fake})
	svc := NewService(server, "gcs1")

	if _, err := svc.DeleteObject(context.Background(), "my-bucket", "report.txt"); err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}
