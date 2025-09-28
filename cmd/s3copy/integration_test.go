package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3UploadDownload(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket"

	_, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	testData := []byte("Hello, MinIO! This is a test file for upload and download via CLI.")
	tempDir := t.TempDir()
	sourceFile := filepath.Join(tempDir, "source.txt")
	err := os.WriteFile(sourceFile, testData, 0644)
	require.NoError(t, err)

	s3Key := "test-file.txt"

	t.Run("upload", func(t *testing.T) {
		setTestConfig(sourceFile, fmt.Sprintf("s3://%s/%s", bucketName, s3Key), bucketName, false, false, false, false)
		err := uploadToS3(ctx)
		assert.NoError(t, err)
	})

	t.Run("download", func(t *testing.T) {
		destFile := filepath.Join(tempDir, "downloaded.txt")
		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, s3Key), destFile, bucketName, false, false, false, false)
		err := downloadFromS3(ctx)
		assert.NoError(t, err)

		downloadedData, err := os.ReadFile(destFile)
		assert.NoError(t, err)
		assert.Equal(t, testData, downloadedData)
	})

	t.Run("list objects", func(t *testing.T) {
		bucket = bucketName
		listObjects = true
		filter = ""
		listDetailed = false
		quiet = false
		verbose = false

		output := captureStdout(func() {
			err := listS3Objects()
			assert.NoError(t, err)
		})
		assert.Contains(t, output, s3Key)
	})
}
