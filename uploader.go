package walg

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"io"
	"log"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

const DataFolderPath = "~/walg_data"

// Uploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader. Must call createUploader()
// in 'upload.go'.
type Uploader struct {
	uploaderApi          s3manageriface.UploaderAPI
	uploadingFolder      *S3Folder
	serverSideEncryption string
	SSEKMSKeyId          string
	StorageClass         string
	Success              bool
	compressor           Compressor
	useWalDelta          bool
	waitGroup            *sync.WaitGroup
}

// NewUploader creates a new tar uploader without the actual
// S3 uploader. createUploader() is used to configure byte size and
// concurrency streams for the uploader.
func NewUploader(uploaderAPI s3manageriface.UploaderAPI, compressor Compressor, uploadingLocation *S3Folder, useWalDelta bool) *Uploader {
	return &Uploader{
		uploaderApi:     uploaderAPI,
		uploadingFolder: uploadingLocation,
		StorageClass:    "STANDARD",
		compressor:      compressor,
		useWalDelta:     useWalDelta,
		waitGroup:       &sync.WaitGroup{},
	}
}

// finish waits for all waiting parts to be uploaded. If an error occurs,
// prints alert to stderr.
func (uploader *Uploader) finish() {
	uploader.waitGroup.Wait()
	if !uploader.Success {
		log.Printf("WAL-G could not complete upload.\n")
	}
}

// Clone creates similar Uploader with new WaitGroup
func (uploader *Uploader) Clone() *Uploader {
	return &Uploader{
		uploader.uploaderApi,
		uploader.uploadingFolder,
		uploader.serverSideEncryption,
		uploader.SSEKMSKeyId,
		uploader.StorageClass,
		uploader.Success,
		uploader.compressor,
		uploader.useWalDelta,
		&sync.WaitGroup{},
	}
}

// TODO : unit tests
// UploadWalFile compresses a WAL file and uploads to S3. Returns
// the first error encountered and an empty string upon failure.
func (uploader *Uploader) UploadWalFile(file NamedReader, verify bool) (string, error) {
	var walFileReader io.Reader

	filename := path.Base(file.Name())
	if uploader.useWalDelta && isWalFilename(filename) {
		recordingReader, err := NewWalDeltaRecordingReader(file, filename, uploader.Clone(), DataFolderPath)
		if err != nil {
			walFileReader = file
		} else {
			walFileReader = recordingReader
			defer recordingReader.Close()
		}
	} else {
		walFileReader = file
	}

	pipeWriter := &CompressingPipeWriter{
		Input:                walFileReader,
		NewCompressingWriter: uploader.compressor.NewWriter,
	}

	pipeWriter.Compress(&OpenPGPCrypter{})

	dstPath := sanitizePath(*uploader.uploadingFolder.Server + WalPath + filepath.Base(file.Name()) + "." + uploader.compressor.FileExtension())
	reader := pipeWriter.Output

	if verify {
		reader = newMd5Reader(reader)
	}

	input := uploader.CreateUploadInput(dstPath, reader)

	err := uploader.upload(input, file.Name())
	fmt.Println("WAL PATH:", dstPath)
	if verify {
		sum := reader.(*MD5Reader).Sum()
		archive := &Archive{
			Folder:  uploader.uploadingFolder,
			Archive: aws.String(dstPath),
		}
		eTag, err := archive.getETag()
		if err != nil {
			log.Fatalf("Unable to verify WAL %s", err)
		}
		if eTag == nil {
			log.Fatalf("Unable to verify WAL: nil ETag ")
		}

		trimETag := strings.Trim(*eTag, "\"")
		if sum != trimETag {
			log.Fatalf("WAL verification failed: md5 %s ETag %s", sum, trimETag)
		}
		fmt.Println("ETag ", trimETag)
	}
	return dstPath, err
}

// CreateUploadInput creates a s3manager.UploadInput for a Uploader using
// the specified path and reader.
func (uploader *Uploader) CreateUploadInput(path string, reader io.Reader) *s3manager.UploadInput {
	uploadInput := &s3manager.UploadInput{
		Bucket:       uploader.uploadingFolder.Bucket,
		Key:          aws.String(path),
		Body:         reader,
		StorageClass: aws.String(uploader.StorageClass),
	}

	if uploader.serverSideEncryption != "" {
		uploadInput.ServerSideEncryption = aws.String(uploader.serverSideEncryption)

		if uploader.SSEKMSKeyId != "" {
			// Only aws:kms implies sseKmsKeyId, checked during validation
			uploadInput.SSEKMSKeyId = aws.String(uploader.SSEKMSKeyId)
		}
	}

	return uploadInput
}

// Helper function to upload to S3. If an error occurs during upload, retries will
// occur in exponentially incremental seconds.
func (uploader *Uploader) upload(input *s3manager.UploadInput, path string) error {
	uploaderAPI := uploader.uploaderApi

	_, err := uploaderAPI.Upload(input)
	if err == nil {
		uploader.Success = true
		return nil
	}

	if multierr, ok := err.(s3manager.MultiUploadFailure); ok {
		log.Printf("upload: failed to upload '%s' with UploadID '%s'.", path, multierr.UploadID())
	} else {
		log.Printf("upload: failed to upload '%s': %s.", path, err.Error())
	}
	return err
}
