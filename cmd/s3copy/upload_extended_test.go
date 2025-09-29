package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUploadToS3SingleFile(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-single-upload-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.txt")
	testContent := []byte("single file content")
	err := os.WriteFile(testFile, testContent, 0644)
	require.NoError(t, err)

	t.Run("upload single file", func(t *testing.T) {
		setTestConfig(testFile, fmt.Sprintf("s3://%s/uploaded.txt", bucketName), bucketName, false, false, true, false)
		err := uploadToS3(ctx)
		assert.NoError(t, err)

		obj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("uploaded.txt"),
		})
		require.NoError(t, err)
		defer closeWithLog(obj.Body, "response body")

		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(obj.Body)
		require.NoError(t, err)
		assert.Equal(t, testContent, buf.Bytes())
	})
}

func TestUploadToS3WithSkipExisting(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-skip-upload-bucket"

	_, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "skip-test.txt")
	testContent := []byte("content for skip test")
	err := os.WriteFile(testFile, testContent, 0644)
	require.NoError(t, err)

	t.Run("skip existing file with same checksum by default", func(t *testing.T) {
		s3Key := "skip-test-key.txt"

		setTestConfig(testFile, fmt.Sprintf("s3://%s/%s", bucketName, s3Key), bucketName, false, false, true, false)
		err := uploadToS3(ctx)
		require.NoError(t, err)

		forceOverwrite = false
		err = uploadToS3(ctx)
		assert.NoError(t, err)

		forceOverwrite = true
		err = uploadToS3(ctx)
		assert.NoError(t, err)
		forceOverwrite = false
	})
}

func TestUploadToS3WithEncryption(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-encrypted-upload-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "encrypt-test.txt")
	originalContent := []byte("secret content to encrypt")
	err := os.WriteFile(testFile, originalContent, 0644)
	require.NoError(t, err)

	t.Run("upload encrypted file", func(t *testing.T) {
		password = "encryption-password-123"
		s3Key := "encrypted-file.txt"

		setTestConfig(testFile, fmt.Sprintf("s3://%s/%s", bucketName, s3Key), bucketName, true, false, true, false)
		err := uploadToS3(ctx)
		assert.NoError(t, err)

		_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Key),
		})
		assert.NoError(t, err)
	})
}

func TestUploadToS3WithGlob(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-glob-upload-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()

	testFiles := []string{
		filepath.Join(tempDir, "test1.txt"),
		filepath.Join(tempDir, "test2.txt"),
		filepath.Join(tempDir, "test3.log"),
	}

	for _, file := range testFiles {
		err := os.WriteFile(file, []byte("content"), 0644)
		require.NoError(t, err)
	}

	t.Run("upload files matching glob pattern", func(t *testing.T) {
		pattern := filepath.Join(tempDir, "*.txt")
		setTestConfig(pattern, fmt.Sprintf("s3://%s/glob-test/", bucketName), bucketName, false, false, true, false)

		err := uploadToS3(ctx)
		assert.NoError(t, err)

		_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("glob-test/test1.txt"),
		})
		assert.NoError(t, err)

		_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("glob-test/test2.txt"),
		})
		assert.NoError(t, err)
	})
}

func TestUploadDirectoryWithIgnore(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-ignore-upload-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()

	testFiles := map[string]string{
		filepath.Join(tempDir, "keep.txt"):        "keep this",
		filepath.Join(tempDir, "ignore.tmp"):      "ignore this",
		filepath.Join(tempDir, "also-keep.log"):   "keep this log",
		filepath.Join(tempDir, "ignore-this.bak"): "ignore backup",
	}

	for file, content := range testFiles {
		err := os.WriteFile(file, []byte(content), 0644)
		require.NoError(t, err)
	}

	t.Run("upload directory with ignore patterns", func(t *testing.T) {
		ignorePatterns = "*.tmp,*.bak"
		err := initializeIgnoreMatcher()
		require.NoError(t, err)

		setTestConfig(tempDir, fmt.Sprintf("s3://%s/filtered/", bucketName), bucketName, false, true, true, false)
		err = uploadToS3(ctx)
		assert.NoError(t, err)

		_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("filtered/keep.txt"),
		})
		assert.NoError(t, err)

		_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("filtered/also-keep.log"),
		})
		assert.NoError(t, err)

		_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("filtered/ignore.tmp"),
		})
		assert.Error(t, err)

		_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("filtered/ignore-this.bak"),
		})
		assert.Error(t, err)

		ignorePatterns = ""
		ignoreMatcher = nil
	})
}

func TestUploadFileWithParams(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-upload-params-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "params-test.txt")
	testContent := []byte("test content for params")
	err := os.WriteFile(testFile, testContent, 0644)
	require.NoError(t, err)

	t.Run("upload with checkSkipExisting false", func(t *testing.T) {
		uploader := manager.NewUploader(s3Client)
		s3Key := "params-test-file.txt"

		err := uploadFileWithParams(ctx, uploader, bucketName, s3Key, testFile, false)
		assert.NoError(t, err)

		_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Key),
		})
		assert.NoError(t, err)
	})
}

func TestUploadDirectoryNested(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-nested-upload-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()

	nestedFiles := []string{
		filepath.Join(tempDir, "file1.txt"),
		filepath.Join(tempDir, "subdir1", "file2.txt"),
		filepath.Join(tempDir, "subdir1", "subdir2", "file3.txt"),
		filepath.Join(tempDir, "subdir1", "subdir2", "file4.txt"),
	}

	for _, file := range nestedFiles {
		err := os.MkdirAll(filepath.Dir(file), 0755)
		require.NoError(t, err)
		err = os.WriteFile(file, []byte("content"), 0644)
		require.NoError(t, err)
	}

	t.Run("upload nested directory structure", func(t *testing.T) {
		setTestConfig(tempDir, fmt.Sprintf("s3://%s/nested/", bucketName), bucketName, false, true, true, false)

		err := uploadToS3(ctx)
		assert.NoError(t, err)

		expectedKeys := []string{
			"nested/file1.txt",
			"nested/subdir1/file2.txt",
			"nested/subdir1/subdir2/file3.txt",
			"nested/subdir1/subdir2/file4.txt",
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
