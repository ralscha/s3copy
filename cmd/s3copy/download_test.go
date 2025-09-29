package main

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadFromS3Directory(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-download-dir-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	testObjects := []string{
		"prefix/file1.txt",
		"prefix/file2.txt",
		"prefix/subdir/file3.txt",
	}

	for _, key := range testObjects {
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("test content")),
		})
		require.NoError(t, err)
	}

	t.Run("download directory", func(t *testing.T) {
		destDir := t.TempDir()
		setTestConfig(fmt.Sprintf("s3://%s/prefix/", bucketName), destDir, bucketName, false, false, true, false)
		err := downloadFromS3(ctx)
		assert.NoError(t, err)

		expectedFiles := []string{
			filepath.Join(destDir, "file1.txt"),
			filepath.Join(destDir, "file2.txt"),
			filepath.Join(destDir, "subdir", "file3.txt"),
		}

		for _, file := range expectedFiles {
			assert.FileExists(t, file)
		}
	})
}
