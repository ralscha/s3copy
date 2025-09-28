package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUploadToS3Errors(t *testing.T) {
	ctx := context.Background()

	restore := preserveGlobalVars()
	defer restore()

	config = Config{
		AccessKey: "dummy",
		SecretKey: "dummy",
		Region:    "us-east-1",
	}

	t.Run("source does not exist", func(t *testing.T) {
		setTestConfig("/nonexistent/file.txt", "s3://bucket/key", "bucket", false, false, true, false)
		err := uploadToS3(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to stat source")
	})

	t.Run("directory without recursive", func(t *testing.T) {
		tempDir := t.TempDir()
		setTestConfig(tempDir, "s3://bucket/key", "bucket", false, false, true, false)
		err := uploadToS3(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "source is a directory, use -r flag")
	})
}

func TestUploadToS3Directory(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-upload-dir-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()
	subDir := filepath.Join(tempDir, "subdir")
	err := os.MkdirAll(subDir, 0755)
	require.NoError(t, err)

	files := []string{
		filepath.Join(tempDir, "file1.txt"),
		filepath.Join(tempDir, "file2.txt"),
		filepath.Join(subDir, "file3.txt"),
	}

	for _, file := range files {
		err := os.WriteFile(file, []byte("test content"), 0644)
		require.NoError(t, err)
	}

	t.Run("upload directory", func(t *testing.T) {
		setTestConfig(tempDir, fmt.Sprintf("s3://%s/test-prefix/", bucketName), bucketName, false, true, true, false)
		err := uploadToS3(ctx)
		assert.NoError(t, err)

		expectedKeys := []string{
			"test-prefix/file1.txt",
			"test-prefix/file2.txt",
			"test-prefix/subdir/file3.txt",
		}

		for _, key := range expectedKeys {
			_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
			})
			assert.NoError(t, err, "File %s should exist in S3", key)
		}
	})
}

func TestUploadToS3MultipleFiles(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-multiple-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()
	files := []string{
		filepath.Join(tempDir, "test1.txt"),
		filepath.Join(tempDir, "test2.txt"),
	}

	for _, file := range files {
		err := os.WriteFile(file, []byte("test content"), 0644)
		require.NoError(t, err)
	}

	t.Run("upload multiple files", func(t *testing.T) {
		pattern := filepath.Join(tempDir, "test*.txt")
		matches, err := filepath.Glob(pattern)
		require.NoError(t, err)
		require.Len(t, matches, 2)

		for i, file := range matches {
			key := fmt.Sprintf("file%d.txt", i+1)
			setTestConfig(file, fmt.Sprintf("s3://%s/%s", bucketName, key), bucketName, false, false, true, false)
			err := uploadToS3(ctx)
			assert.NoError(t, err)
		}

		expectedKeys := []string{
			"file1.txt",
			"file2.txt",
		}

		for _, key := range expectedKeys {
			_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
			})
			assert.NoError(t, err, "File %s should exist in S3", key)
		}
	})
}
