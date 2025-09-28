package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseS3Path(t *testing.T) {
	tests := []struct {
		name           string
		s3Path         string
		providedBucket string
		isDir          bool
		localPath      string
		expectedBucket string
		expectedKey    string
		expectError    bool
	}{
		{
			name:           "full s3 path",
			s3Path:         "s3://mybucket/path/to/file.txt",
			providedBucket: "",
			isDir:          false,
			localPath:      "/tmp/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "path/to/file.txt",
			expectError:    false,
		},
		{
			name:           "bucket only with provided bucket",
			s3Path:         "s3://mybucket",
			providedBucket: "",
			isDir:          false,
			localPath:      "/tmp/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "file.txt",
			expectError:    false,
		},
		{
			name:           "with provided bucket",
			s3Path:         "s3://mybucket/path",
			providedBucket: "otherbucket",
			isDir:          false,
			localPath:      "/tmp/file.txt",
			expectedBucket: "otherbucket",
			expectedKey:    "mybucket/path",
			expectError:    false,
		},
		{
			name:           "directory path",
			s3Path:         "s3://mybucket/path/",
			providedBucket: "",
			isDir:          true,
			localPath:      "/tmp/dir",
			expectedBucket: "mybucket",
			expectedKey:    "path/",
			expectError:    false,
		},
		{
			name:           "directory without bucket",
			s3Path:         "s3://",
			providedBucket: "",
			isDir:          true,
			localPath:      "/tmp/dir",
			expectError:    true,
		},
		{
			name:           "bucket with trailing slash, no provided bucket, not dir",
			s3Path:         "s3://mybucket/",
			providedBucket: "",
			isDir:          false,
			localPath:      "/tmp/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "file.txt",
			expectError:    false,
		},
		{
			name:           "provided bucket with trailing slash",
			s3Path:         "s3://otherbucket/",
			providedBucket: "otherbucket",
			isDir:          false,
			localPath:      "/tmp/file.txt",
			expectedBucket: "otherbucket",
			expectedKey:    "file.txt",
			expectError:    false,
		},
		{
			name:           "s3 path with directory and trailing slash - single copy mode",
			s3Path:         "s3://mybucket/path/to/dir/",
			providedBucket: "",
			isDir:          false,
			localPath:      "/tmp/myfile.txt",
			expectedBucket: "mybucket",
			expectedKey:    "path/to/dir/myfile.txt",
			expectError:    false,
		},
		{
			name:           "provided bucket with directory and trailing slash - single copy mode",
			s3Path:         "s3://mybucket/some/folder/",
			providedBucket: "mybucket",
			isDir:          false,
			localPath:      "/local/path/document.pdf",
			expectedBucket: "mybucket",
			expectedKey:    "some/folder/document.pdf",
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := parseS3Path(tt.s3Path, tt.providedBucket, tt.isDir, tt.localPath)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedBucket, bucket)
				assert.Equal(t, tt.expectedKey, key)
			}
		})
	}
}

func TestCheckS3ObjectExists(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-check-exists-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	t.Run("object exists", func(t *testing.T) {
		key := "test-exists.txt"
		content := []byte("test content for existence check")

		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader(content),
		})
		require.NoError(t, err)

		exists, etag, metadata, err := checkS3ObjectExists(ctx, s3Client, bucketName, key)
		assert.NoError(t, err)
		assert.True(t, exists)
		assert.NotEmpty(t, etag)
		_ = metadata // metadata may be nil or empty for simple uploads
	})

	t.Run("object does not exist", func(t *testing.T) {
		key := "non-existent-key.txt"

		exists, etag, metadata, err := checkS3ObjectExists(ctx, s3Client, bucketName, key)
		_ = metadata // metadata may be nil for non-existent objects
		if err != nil {
			t.Logf("Error returned: %v", err)
			assert.Contains(t, err.Error(), "404")
			assert.False(t, exists)
			assert.Empty(t, etag)
		} else {
			assert.False(t, exists)
			assert.Empty(t, etag)
		}
	})

	t.Run("bucket does not exist", func(t *testing.T) {
		key := "test-key.txt"
		nonExistentBucket := "non-existent-bucket"

		exists, etag, metadata, err := checkS3ObjectExists(ctx, s3Client, nonExistentBucket, key)
		if err != nil {
			assert.False(t, exists)
			assert.Empty(t, etag)
			assert.Empty(t, metadata)
		} else {
			assert.False(t, exists)
		}
	})
}

func TestListS3ObjectsDetailed(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	testObjects := []struct {
		key  string
		size int64
	}{
		{"file1.txt", 100},
		{"file2.txt", 200},
		{"dir/file3.txt", 300},
	}

	for _, obj := range testObjects {
		data := make([]byte, obj.size)
		_, err := rand.Read(data)
		require.NoError(t, err)

		_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(obj.key),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)
	}

	t.Run("detailed listing", func(t *testing.T) {
		bucket = bucketName
		listObjects = true
		filter = ""
		listDetailed = true
		quiet = false
		verbose = false

		output := captureStdout(func() {
			err := listS3Objects()
			assert.NoError(t, err)
		})
		assert.Contains(t, output, "file1.txt")
		assert.Contains(t, output, "file2.txt")
		assert.Contains(t, output, "dir/file3.txt")
		assert.Contains(t, output, "Storage Class")
		assert.Contains(t, output, "ETag")
	})

	t.Run("listing with filter", func(t *testing.T) {
		bucket = bucketName
		listObjects = true
		filter = "dir/"
		listDetailed = false
		quiet = false
		verbose = false

		output := captureStdout(func() {
			err := listS3Objects()
			assert.NoError(t, err)
		})
		assert.NotContains(t, output, "file1.txt")
		assert.NotContains(t, output, "file2.txt")
		assert.Contains(t, output, "dir/file3.txt")
	})
}
