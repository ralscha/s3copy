package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	manager "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadFromS3SingleFile(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-download-single-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	testContent := []byte("test content for single file download")
	testKey := "test-file.txt"

	_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testKey),
		Body:   bytes.NewReader(testContent),
	})
	require.NoError(t, err)

	t.Run("download single file to specific path", func(t *testing.T) {
		destFile := filepath.Join(t.TempDir(), "downloaded.txt")
		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, testKey), destFile, bucketName, false, false, true, false)

		err := downloadFromS3(ctx)
		assert.NoError(t, err)
		assert.FileExists(t, destFile)

		content, err := os.ReadFile(destFile)
		require.NoError(t, err)
		assert.Equal(t, testContent, content)
	})

	t.Run("download single file to directory", func(t *testing.T) {
		destDir := t.TempDir()
		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, testKey), destDir, bucketName, false, false, true, false)

		err := downloadFromS3(ctx)
		assert.NoError(t, err)

		expectedFile := filepath.Join(destDir, testKey)
		assert.FileExists(t, expectedFile)

		content, err := os.ReadFile(expectedFile)
		require.NoError(t, err)
		assert.Equal(t, testContent, content)
	})

	t.Run("download single file with trailing slash", func(t *testing.T) {
		destDir := t.TempDir()
		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, testKey), destDir+"/", bucketName, false, false, true, false)

		err := downloadFromS3(ctx)
		assert.NoError(t, err)

		expectedFile := filepath.Join(destDir, testKey)
		assert.FileExists(t, expectedFile)
	})
}

func TestDownloadFromS3WithSkipExisting(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-skip-existing-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	testContent := []byte("test content for skip existing")
	testKey := "skip-test.txt"

	_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testKey),
		Body:   bytes.NewReader(testContent),
	})
	require.NoError(t, err)

	t.Run("skip existing file with same checksum by default", func(t *testing.T) {
		destDir := t.TempDir()
		destFile := filepath.Join(destDir, "skip-test.txt")

		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, testKey), destFile, bucketName, false, false, true, false)
		err := downloadFromS3(ctx)
		require.NoError(t, err)

		forceOverwrite = false
		err = downloadFromS3(ctx)
		assert.NoError(t, err)

		forceOverwrite = true
		err = downloadFromS3(ctx)
		assert.NoError(t, err)
		forceOverwrite = false
	})
}

func TestDownloadFromS3WithEncryption(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-encrypted-download-bucket"

	_, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	originalContent := []byte("secret content to encrypt")
	testKey := "encrypted-file.txt"
	password = "test-encryption-password"

	tempFile := filepath.Join(t.TempDir(), "source.txt")
	err := os.WriteFile(tempFile, originalContent, 0644)
	require.NoError(t, err)

	setTestConfig(tempFile, fmt.Sprintf("s3://%s/%s", bucketName, testKey), bucketName, true, false, true, false)
	err = uploadToS3(ctx)
	require.NoError(t, err)

	t.Run("download and decrypt file", func(t *testing.T) {
		destFile := filepath.Join(t.TempDir(), "decrypted.txt")
		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, testKey), destFile, bucketName, true, false, true, false)

		err := downloadFromS3(ctx)
		assert.NoError(t, err)
		assert.FileExists(t, destFile)

		content, err := os.ReadFile(destFile)
		require.NoError(t, err)
		assert.Equal(t, originalContent, content)
	})

	t.Run("failed decrypt preserves existing file", func(t *testing.T) {
		destFile := filepath.Join(t.TempDir(), "existing.txt")
		existingContent := []byte("keep-existing-content")
		err := os.WriteFile(destFile, existingContent, 0644)
		require.NoError(t, err)

		password = "wrong-password"
		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, testKey), destFile, bucketName, true, false, true, false)

		err = downloadFromS3(ctx)
		require.Error(t, err)

		content, readErr := os.ReadFile(destFile)
		require.NoError(t, readErr)
		assert.Equal(t, existingContent, content)
	})
}

func TestDownloadFromS3Errors(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-download-errors-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	t.Run("download non-existent object", func(t *testing.T) {
		destFile := filepath.Join(t.TempDir(), "output.txt")
		setTestConfig(fmt.Sprintf("s3://%s/nonexistent-key.txt", bucketName), destFile, bucketName, false, false, true, false)

		err := downloadFromS3(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no objects found")
	})

	t.Run("download with invalid S3 path", func(t *testing.T) {
		destFile := filepath.Join(t.TempDir(), "output.txt")
		setTestConfig("s3://", destFile, "", false, false, true, false)

		err := downloadFromS3(ctx)
		assert.Error(t, err)
	})

	t.Run("download to invalid destination", func(t *testing.T) {
		testKey := "test-file.txt"
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader([]byte("test")),
		})
		require.NoError(t, err)

		if os.Getenv("CI") == "" {
			setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, testKey), "/invalid/path/file.txt", bucketName, false, false, true, false)
			err = downloadFromS3(ctx)
			assert.Error(t, err)
		}
	})
}

func TestDownloadFileWithParams(t *testing.T) {
	restore := preserveGlobalVars()
	defer restore()
	encrypt = false
	dryRun = false
	forceOverwrite = false

	ctx := context.Background()
	bucketName := "test-download-params-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	testKey := "params-test.txt"
	testContent := []byte("content for params test")

	_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testKey),
		Body:   bytes.NewReader(testContent),
	})
	require.NoError(t, err)

	t.Run("download with checkSkipExisting false", func(t *testing.T) {
		destFile := filepath.Join(t.TempDir(), "output.txt")
		downloader := manager.New(s3Client)

		err := downloadFileWithParams(ctx, downloader, bucketName, testKey, destFile, false)
		assert.NoError(t, err)
		assert.FileExists(t, destFile)
	})

	t.Run("download with dry run", func(t *testing.T) {
		destFile := filepath.Join(t.TempDir(), "output-dryrun.txt")
		downloader := manager.New(s3Client)

		dryRun = true
		err := downloadFileWithParams(ctx, downloader, bucketName, testKey, destFile, true)
		assert.NoError(t, err)
		assert.NoFileExists(t, destFile)
		dryRun = false
	})

	t.Run("failed download preserves existing local file", func(t *testing.T) {
		destFile := filepath.Join(t.TempDir(), "existing.txt")
		originalContent := []byte("keep-me")
		err := os.WriteFile(destFile, originalContent, 0644)
		require.NoError(t, err)

		downloader := manager.New(s3Client)
		err = downloadFileWithParams(ctx, downloader, bucketName, "does-not-exist.txt", destFile, false)
		require.Error(t, err)

		content, err := os.ReadFile(destFile)
		require.NoError(t, err)
		assert.Equal(t, originalContent, content)
	})
}

func TestDownloadDirectory(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-download-directory-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	testFiles := map[string][]byte{
		"dir/file1.txt":       []byte("content 1"),
		"dir/file2.txt":       []byte("content 2"),
		"dir/sub/file3.txt":   []byte("content 3"),
		"dir/sub/file4.txt":   []byte("content 4"),
		"other/unrelated.txt": []byte("unrelated"),
	}

	for key, content := range testFiles {
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader(content),
		})
		require.NoError(t, err)
	}

	t.Run("download directory recursively", func(t *testing.T) {
		destDir := t.TempDir()
		setTestConfig(fmt.Sprintf("s3://%s/dir/", bucketName), destDir, bucketName, false, false, true, false)

		err := downloadFromS3(ctx)
		assert.NoError(t, err)

		expectedFiles := []string{
			filepath.Join(destDir, "file1.txt"),
			filepath.Join(destDir, "file2.txt"),
			filepath.Join(destDir, "sub", "file3.txt"),
			filepath.Join(destDir, "sub", "file4.txt"),
		}

		for _, expectedFile := range expectedFiles {
			assert.FileExists(t, expectedFile)
		}

		unexpectedFile := filepath.Join(destDir, "other", "unrelated.txt")
		assert.NoFileExists(t, unexpectedFile)
	})
}

func TestDownloadDirectoryWithPagination(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-download-pagination-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	const fileCount = 1105
	for i := range fileCount {
		key := fmt.Sprintf("paged/file-%04d.txt", i)
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("content")),
		})
		require.NoError(t, err)
	}

	destDir := t.TempDir()
	setTestConfig(fmt.Sprintf("s3://%s/paged/", bucketName), destDir, bucketName, false, false, true, false)

	err := downloadFromS3(ctx)
	require.NoError(t, err)

	entries, err := os.ReadDir(destDir)
	require.NoError(t, err)
	assert.Len(t, entries, fileCount)

	assert.FileExists(t, filepath.Join(destDir, "file-0000.txt"))
	assert.FileExists(t, filepath.Join(destDir, "file-1104.txt"))
}
