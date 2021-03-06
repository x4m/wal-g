package test

import (
	"archive/tar"
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/x4m/wal-g/internal"
	"github.com/x4m/wal-g/testtools"
	"io"
	"strings"
	"testing"
)

// TODO : this test is broken now
// Tests S3 get and set methods.
func TestS3TarBall(t *testing.T) {
	bundle := &internal.Bundle{
		ArchiveDirectory: "/usr/local",
		TarSizeThreshold: int64(10),
	}

	bundle.TarBallMaker = internal.NewStorageTarBallMaker("test", testtools.NewMockUploader(false, false))

	bundle.NewTarBall(false)

	assert.NotNil(t, bundle.TarBall)

	tarBall := bundle.TarBall

	assert.Equal(t, int64(0), tarBall.Size())
	assert.Nil(t, tarBall.TarWriter())

	bundle.NewTarBall(false)
	//assert.Equal(t, bundle.TarBall, tarBall)
}

// Tests S3 dependent functions for StorageTarBall such as
// SetUp(), CloseTar() and Finish().
func TestS3DependentFunctions(t *testing.T) {
	bundle := &internal.Bundle{
		ArchiveDirectory: "",
		TarSizeThreshold: 100,
	}

	uploader := testtools.NewMockUploader(false, false)

	bundle.TarBallMaker = internal.NewStorageTarBallMaker("mockBackup", uploader)

	bundle.NewTarBall(false)
	tarBall := bundle.TarBall
	tarBall.SetUp(MockArmedCrypter())
	tarWriter := tarBall.TarWriter()

	mockData := []byte("a")

	// Write mock header.
	mockHeader := &tar.Header{
		Name: "mock",
		Size: int64(len(mockData)),
	}
	err := tarWriter.WriteHeader(mockHeader)
	if err != nil {
		t.Log(err)
	}

	// Write body.
	_, err = tarWriter.Write(mockData)

	assert.NoError(t, err)
	tarBall.CloseTar()

	// Handle write after close.
	_, err = tarBall.TarWriter().Write(mockData)
	assert.Error(t, err)

	err = tarBall.Finish(&internal.BackupSentinelDto{})
	assert.NoError(t, err)

	// Test naming property of SetUp().
	bundle.NewTarBall(false)
	tarBall = bundle.TarBall
	tarBall.SetUp(MockArmedCrypter(), "mockTarball")
	tarBall.CloseTar()
	err = tarBall.Finish(&internal.BackupSentinelDto{})
	assert.NoError(t, err)
}

func TestPackFileTo(t *testing.T) {
	mockData := "mock"
	mockHeader := &tar.Header{
		Name:     "mock",
		Mode:     int64(0600),
		Size:     int64(len(mockData)),
		Typeflag: tar.TypeReg,
	}
	buffer := bytes.NewBuffer(make([]byte, 0))
	tarBallMaker := testtools.BufferTarBallMaker{
		BufferToWrite: buffer,
	}
	tarBall := tarBallMaker.Make(false)
	tarBall.SetUp(MockDisarmedCrypter())
	size, err := internal.PackFileTo(tarBall, mockHeader, strings.NewReader(mockData))
	assert.Equal(t, int64(len(mockData)), size)
	assert.NoError(t, err)
	assert.Equal(t, tarBall.Size(), size)

	reader := tar.NewReader(buffer)
	interpreter := testtools.BufferTarInterpreter{}
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		err = interpreter.Interpret(reader, header)
		assert.NoError(t, err)
	}
	assert.Equal(t, []byte(mockData), interpreter.Out)
}
