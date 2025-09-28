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

func TestWriteAtWrapper(t *testing.T) {
	t.Run("sequential writes", func(t *testing.T) {
		buf := &bytes.Buffer{}
		wrapper := &writeAtWrapper{w: buf}

		data1 := []byte("hello")
		n1, err1 := wrapper.WriteAt(data1, 0)
		assert.NoError(t, err1)
		assert.Equal(t, 5, n1)

		data2 := []byte(" world")
		n2, err2 := wrapper.WriteAt(data2, 5)
		assert.NoError(t, err2)
		assert.Equal(t, 6, n2)

		assert.Equal(t, "hello world", buf.String())
	})

	t.Run("non-sequential write", func(t *testing.T) {
		buf := &bytes.Buffer{}
		wrapper := &writeAtWrapper{w: buf}

		_, err := wrapper.WriteAt([]byte("hello"), 0)
		assert.NoError(t, err)

		_, err = wrapper.WriteAt([]byte("world"), 10)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non-sequential write")
	})
}
