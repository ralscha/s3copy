package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncMode(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "s3copy-sync-test")
	require.NoError(t, err)
	defer func() {
		if removeErr := os.RemoveAll(tempDir); removeErr != nil {
			t.Logf("Warning: failed to remove temp dir %s: %v", tempDir, removeErr)
		}
	}()

	testFiles := []string{
		"file1.txt",
		"subdir/file2.txt",
		"subdir/file3.txt",
	}

	for _, file := range testFiles {
		fullPath := filepath.Join(tempDir, file)
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		require.NoError(t, err)

		err = os.WriteFile(fullPath, []byte("test content "+file), 0644)
		require.NoError(t, err)
	}

	files, err := listLocalFiles(tempDir)
	assert.NoError(t, err)
	assert.Len(t, files, 3)

	for _, file := range files {
		assert.NotEmpty(t, file.Path)
		assert.NotEmpty(t, file.RelPath)
		assert.NotEmpty(t, file.MD5Hash)
		assert.Greater(t, file.Size, int64(0))
		assert.False(t, file.IsDir)
	}
}

func TestFilesAreSame(t *testing.T) {
	file1 := FileInfo{
		Size:    100,
		MD5Hash: "abc123",
	}

	file2 := FileInfo{
		Size:    100,
		MD5Hash: "abc123",
	}

	file3 := FileInfo{
		Size:    200,
		MD5Hash: "abc123",
	}

	file4 := FileInfo{
		Size:    100,
		MD5Hash: "def456",
	}

	assert.True(t, filesAreSame(file1, file2))
	assert.False(t, filesAreSame(file1, file3)) // Different sizes
	assert.False(t, filesAreSame(file1, file4)) // Different hashes
}

func TestSyncResultSummary(t *testing.T) {
	result := SyncResult{
		Uploaded:   []string{"file1.txt", "file2.txt"},
		Downloaded: []string{"file3.txt"},
		Deleted:    []string{"file4.txt"},
		Errors:     []string{"error with file6.txt"},
	}

	quiet = true // Suppress output during test
	printSyncSummary(result)
	quiet = false
}

func TestSyncLocalToS3(t *testing.T) {
	ctx := context.Background()
	bucketName := "sync-test-bucket"

	restore := preserveGlobalVars()
	defer restore()

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()
	testFiles := map[string]string{
		"file1.txt":            "Content of file 1",
		"file2.txt":            "Content of file 2",
		"subdir/file3.txt":     "Content of file 3 in subdirectory",
		"subdir/file4.txt":     "Content of file 4 in subdirectory",
		"deep/nested/file.txt": "Content in deeply nested directory",
	}

	for relPath, content := range testFiles {
		fullPath := filepath.Join(tempDir, relPath)
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		require.NoError(t, err)
		err = os.WriteFile(fullPath, []byte(content), 0644)
		require.NoError(t, err)
	}

	t.Run("Initial sync - upload all files", func(t *testing.T) {
		source = tempDir
		destination = fmt.Sprintf("s3://%s/sync-test/", bucketName)
		bucket = bucketName
		recursive = true
		quiet = true

		result, err := syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)

		assert.Len(t, result.Uploaded, len(testFiles))
		assert.Empty(t, result.Downloaded)
		assert.Empty(t, result.Deleted)
		assert.Empty(t, result.Errors)

		for relPath := range testFiles {
			_, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("sync-test/" + relPath),
			})
			assert.NoError(t, err, "File %s should exist in S3", relPath)
		}
	})

	t.Run("No changes sync - all files already in sync", func(t *testing.T) {
		result, err := syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)

		assert.Empty(t, result.Uploaded)
		assert.Empty(t, result.Downloaded)
		assert.Empty(t, result.Deleted)
		assert.Empty(t, result.Errors)
	})

	t.Run("Modified file sync", func(t *testing.T) {
		modifiedFile := filepath.Join(tempDir, "file1.txt")
		newContent := "Modified content of file 1"
		err := os.WriteFile(modifiedFile, []byte(newContent), 0644)
		require.NoError(t, err)

		result, err := syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)

		assert.Len(t, result.Uploaded, 1)
		assert.Contains(t, result.Uploaded, "file1.txt")
		assert.Empty(t, result.Downloaded)
		assert.Empty(t, result.Deleted)
		assert.Empty(t, result.Errors)

		obj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("sync-test/file1.txt"),
		})
		require.NoError(t, err)
		defer func() {
			if closeErr := obj.Body.Close(); closeErr != nil {
				t.Logf("Warning: failed to close response body: %v", closeErr)
			}
		}()

		content := make([]byte, len(newContent)+10) // Buffer a bit larger to handle any extra data
		n, err := obj.Body.Read(content)
		if err != nil && err.Error() != "EOF" {
			require.NoError(t, err)
		}

		assert.Equal(t, newContent, string(content[:n]))
	})

	t.Run("New file sync", func(t *testing.T) {
		newFile := filepath.Join(tempDir, "new_file.txt")
		newContent := "This is a new file"
		err := os.WriteFile(newFile, []byte(newContent), 0644)
		require.NoError(t, err)

		result, err := syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)

		assert.Len(t, result.Uploaded, 1)
		assert.Contains(t, result.Uploaded, "new_file.txt")
		assert.Empty(t, result.Downloaded)
		assert.Empty(t, result.Deleted)
		assert.Empty(t, result.Errors)
	})

	t.Run("Deleted file sync", func(t *testing.T) {
		deletedFile := filepath.Join(tempDir, "file2.txt")
		err := os.Remove(deletedFile)
		require.NoError(t, err)

		result, err := syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)

		assert.Empty(t, result.Uploaded)
		assert.Empty(t, result.Downloaded)
		assert.Len(t, result.Deleted, 1)
		assert.Contains(t, result.Deleted, "file2.txt")
		assert.Empty(t, result.Errors)

		_, err = s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("sync-test/file2.txt"),
		})
		assert.Error(t, err, "File should be deleted from S3")
	})
}

func TestSyncS3ToLocal(t *testing.T) {
	ctx := context.Background()
	bucketName := "sync-s3-to-local-bucket"

	restore := preserveGlobalVars()
	defer restore()

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	s3Prefix := "test-data/"
	testFiles := map[string]string{
		"document1.txt":       "Important document content",
		"document2.txt":       "Another important document",
		"reports/report1.pdf": "PDF report content",
		"reports/report2.pdf": "Another PDF report",
		"images/photo1.jpg":   "JPEG image data",
		"images/photo2.png":   "PNG image data",
	}

	for relPath, content := range testFiles {
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Prefix + relPath),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err)
	}

	tempDir := t.TempDir()

	t.Run("Initial sync - download all files", func(t *testing.T) {
		source = fmt.Sprintf("s3://%s/%s", bucketName, s3Prefix)
		destination = tempDir
		bucket = bucketName
		recursive = true
		quiet = true

		result, err := syncS3ToLocal(ctx, s3Client)
		require.NoError(t, err)

		assert.Empty(t, result.Uploaded)
		assert.Len(t, result.Downloaded, len(testFiles))
		assert.Empty(t, result.Deleted)
		assert.Empty(t, result.Errors)

		for relPath, expectedContent := range testFiles {
			localFile := filepath.Join(tempDir, relPath)
			require.FileExists(t, localFile)

			content, err := os.ReadFile(localFile)
			require.NoError(t, err)
			assert.Equal(t, expectedContent, string(content))
		}
	})

	t.Run("No changes sync - all files already in sync", func(t *testing.T) {
		result, err := syncS3ToLocal(ctx, s3Client)
		require.NoError(t, err)

		assert.Empty(t, result.Uploaded)
		assert.Empty(t, result.Downloaded)
		assert.Empty(t, result.Deleted)
		assert.Empty(t, result.Errors)
	})

	t.Run("Modified S3 file sync", func(t *testing.T) {
		modifiedContent := "Updated document content from S3"
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Prefix + "document1.txt"),
			Body:   strings.NewReader(modifiedContent),
		})
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		result, err := syncS3ToLocal(ctx, s3Client)
		require.NoError(t, err)

		assert.Empty(t, result.Uploaded)
		assert.Len(t, result.Downloaded, 1)
		assert.Contains(t, result.Downloaded, "document1.txt")
		assert.Empty(t, result.Deleted)
		assert.Empty(t, result.Errors)

		localFile := filepath.Join(tempDir, "document1.txt")
		content, err := os.ReadFile(localFile)
		require.NoError(t, err)
		assert.Equal(t, modifiedContent, string(content))
	})

	t.Run("New S3 file sync", func(t *testing.T) {
		newContent := "Brand new file from S3"
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Prefix + "new_s3_file.txt"),
			Body:   strings.NewReader(newContent),
		})
		require.NoError(t, err)

		result, err := syncS3ToLocal(ctx, s3Client)
		require.NoError(t, err)

		assert.Empty(t, result.Uploaded)
		assert.Len(t, result.Downloaded, 1)
		assert.Contains(t, result.Downloaded, "new_s3_file.txt")
		assert.Empty(t, result.Deleted)
		assert.Empty(t, result.Errors)

		localFile := filepath.Join(tempDir, "new_s3_file.txt")
		require.FileExists(t, localFile)

		content, err := os.ReadFile(localFile)
		require.NoError(t, err)
		assert.Equal(t, newContent, string(content))
	})

	t.Run("Deleted S3 file sync", func(t *testing.T) {
		_, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Prefix + "document2.txt"),
		})
		require.NoError(t, err)

		result, err := syncS3ToLocal(ctx, s3Client)
		require.NoError(t, err)

		assert.Empty(t, result.Uploaded)
		assert.Empty(t, result.Downloaded)
		assert.Len(t, result.Deleted, 1)
		assert.Contains(t, result.Deleted, "document2.txt")
		assert.Empty(t, result.Errors)

		localFile := filepath.Join(tempDir, "document2.txt")
		assert.NoFileExists(t, localFile)
	})
}

func TestSyncWithIgnorePatternsMinIO(t *testing.T) {
	ctx := context.Background()
	bucketName := "sync-ignore-test-bucket"

	restore := preserveGlobalVars()
	defer restore()

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()

	testFiles := map[string]string{
		"important.txt":       "Important file content",
		"config.json":         "Configuration data",
		"temp.tmp":            "Temporary file",
		"cache.cache":         "Cache file",
		"logs/app.log":        "Application log",
		"logs/error.log":      "Error log",
		"data/users.csv":      "User data",
		"data/backup.bak":     "Backup file",
		".hidden":             "Hidden file",
		"node_modules/lib.js": "Node module file",
	}

	for relPath, content := range testFiles {
		fullPath := filepath.Join(tempDir, relPath)
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		require.NoError(t, err)
		err = os.WriteFile(fullPath, []byte(content), 0644)
		require.NoError(t, err)
	}

	t.Run("Sync with ignore patterns", func(t *testing.T) {
		ignorePatterns = "*.tmp,*.cache,*.log,*.bak,.*,node_modules/"
		err := initializeIgnoreMatcher()
		require.NoError(t, err)

		source = tempDir
		destination = fmt.Sprintf("s3://%s/filtered-sync/", bucketName)
		bucket = bucketName
		recursive = true
		quiet = true

		result, err := syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)

		expectedFiles := []string{"important.txt", "config.json", "data/users.csv"}
		assert.Len(t, result.Uploaded, len(expectedFiles))

		for _, expectedFile := range expectedFiles {
			assert.Contains(t, result.Uploaded, expectedFile)
		}

		ignoredFiles := []string{"temp.tmp", "cache.cache", "logs/app.log", "logs/error.log", "data/backup.bak", ".hidden", "node_modules/lib.js"}
		for _, ignoredFile := range ignoredFiles {
			_, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("filtered-sync/" + ignoredFile),
			})
			assert.Error(t, err, "Ignored file %s should not exist in S3", ignoredFile)
		}
	})
}

func TestSyncBidirectional(t *testing.T) {
	ctx := context.Background()
	bucketName := "bidirectional-sync-bucket"

	restore := preserveGlobalVars()
	defer restore()

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	localDir1 := filepath.Join(t.TempDir(), "env1")
	localDir2 := filepath.Join(t.TempDir(), "env2")

	err := os.MkdirAll(localDir1, 0755)
	require.NoError(t, err)
	err = os.MkdirAll(localDir2, 0755)
	require.NoError(t, err)

	s3Prefix := "shared-data/"

	t.Run("Initial sync from env1 to S3", func(t *testing.T) {
		env1Files := map[string]string{
			"shared1.txt":   "File from environment 1",
			"shared2.txt":   "Another file from env 1",
			"env1_only.txt": "This file exists only in env1",
		}

		for relPath, content := range env1Files {
			fullPath := filepath.Join(localDir1, relPath)
			err := os.WriteFile(fullPath, []byte(content), 0644)
			require.NoError(t, err)
		}

		source = localDir1
		destination = fmt.Sprintf("s3://%s/%s", bucketName, s3Prefix)
		bucket = bucketName
		recursive = true
		quiet = true

		result, err := syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)

		assert.Len(t, result.Uploaded, 3)
		assert.Empty(t, result.Errors)
	})

	t.Run("Sync from S3 to env2", func(t *testing.T) {
		source = fmt.Sprintf("s3://%s/%s", bucketName, s3Prefix)
		destination = localDir2
		bucket = bucketName
		recursive = true
		quiet = true

		result, err := syncS3ToLocal(ctx, s3Client)
		require.NoError(t, err)

		assert.Len(t, result.Downloaded, 3)
		assert.Empty(t, result.Errors)

		expectedFiles := []string{"shared1.txt", "shared2.txt", "env1_only.txt"}
		for _, filename := range expectedFiles {
			localFile := filepath.Join(localDir2, filename)
			assert.FileExists(t, localFile)
		}
	})

	t.Run("Make changes in both environments and sync", func(t *testing.T) {
		err := os.WriteFile(filepath.Join(localDir1, "shared1.txt"), []byte("Modified in env1"), 0644)
		require.NoError(t, err)

		err = os.WriteFile(filepath.Join(localDir2, "env2_only.txt"), []byte("New file from env2"), 0644)
		require.NoError(t, err)

		source = localDir1
		destination = fmt.Sprintf("s3://%s/%s", bucketName, s3Prefix)

		result, err := syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)
		assert.Len(t, result.Uploaded, 1) // Only the modified file
		assert.Contains(t, result.Uploaded, "shared1.txt")

		source = localDir2
		destination = fmt.Sprintf("s3://%s/%s", bucketName, s3Prefix)

		result, err = syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(result.Uploaded), 1) // At least the new file
		assert.Contains(t, result.Uploaded, "env2_only.txt")

		source = fmt.Sprintf("s3://%s/%s", bucketName, s3Prefix)
		destination = localDir1

		result, err = syncS3ToLocal(ctx, s3Client)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(result.Downloaded), 1)

		env2File := filepath.Join(localDir1, "env2_only.txt")
		assert.FileExists(t, env2File)

		content, err := os.ReadFile(env2File)
		require.NoError(t, err)
		assert.Equal(t, "New file from env2", string(content))
	})
}

func TestSyncLargeFiles(t *testing.T) {
	ctx := context.Background()
	bucketName := "large-files-sync-bucket"

	restore := preserveGlobalVars()
	defer restore()

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()

	t.Run("Sync large files", func(t *testing.T) {
		testFiles := map[string]int{
			"small.txt":  1024,             // 1KB
			"medium.txt": 1024 * 1024,      // 1MB
			"large.txt":  10 * 1024 * 1024, // 10MB
		}

		for filename, size := range testFiles {
			fullPath := filepath.Join(tempDir, filename)
			file, err := os.Create(fullPath)
			require.NoError(t, err)

			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}
			_, err = file.Write(data)
			require.NoError(t, err)
			if closeErr := file.Close(); closeErr != nil {
				t.Fatalf("Failed to close file: %v", closeErr)
			}
		}

		source = tempDir
		destination = fmt.Sprintf("s3://%s/large-files/", bucketName)
		bucket = bucketName
		recursive = true
		quiet = true

		result, err := syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)

		assert.Len(t, result.Uploaded, len(testFiles))
		assert.Empty(t, result.Errors)

		for filename, expectedSize := range testFiles {
			obj, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("large-files/" + filename),
			})
			require.NoError(t, err)
			assert.Equal(t, int64(expectedSize), *obj.ContentLength)
		}
	})

	t.Run("Download large files", func(t *testing.T) {
		downloadDir := filepath.Join(t.TempDir(), "downloads")
		err := os.MkdirAll(downloadDir, 0755)
		require.NoError(t, err)

		source = fmt.Sprintf("s3://%s/large-files/", bucketName)
		destination = downloadDir
		bucket = bucketName
		recursive = true
		quiet = true

		result, err := syncS3ToLocal(ctx, s3Client)
		require.NoError(t, err)

		assert.Len(t, result.Downloaded, 3)
		assert.Empty(t, result.Errors)

		testFiles := map[string]int{
			"small.txt":  1024,
			"medium.txt": 1024 * 1024,
			"large.txt":  10 * 1024 * 1024,
		}

		for filename, expectedSize := range testFiles {
			fullPath := filepath.Join(downloadDir, filename)
			info, err := os.Stat(fullPath)
			require.NoError(t, err)
			assert.Equal(t, int64(expectedSize), info.Size())
		}
	})
}

func TestSyncErrorHandling(t *testing.T) {
	ctx := context.Background()
	bucketName := "error-handling-bucket"

	restore := preserveGlobalVars()
	defer restore()

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()

	t.Run("Sync to non-existent bucket", func(t *testing.T) {
		testFile := filepath.Join(tempDir, "test.txt")
		err := os.WriteFile(testFile, []byte("test content"), 0644)
		require.NoError(t, err)

		source = tempDir
		destination = "s3://non-existent-bucket/test/"
		bucket = "non-existent-bucket"
		recursive = true
		quiet = true

		_, err = syncLocalToS3(ctx, s3Client)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to list S3 files")
	})

	t.Run("Sync empty directory", func(t *testing.T) {
		emptyDir := filepath.Join(t.TempDir(), "empty")
		err := os.MkdirAll(emptyDir, 0755)
		require.NoError(t, err)

		source = emptyDir
		destination = fmt.Sprintf("s3://%s/empty-test/", bucketName)
		bucket = bucketName
		recursive = true
		quiet = true

		result, err := syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)

		assert.Empty(t, result.Uploaded)
		assert.Empty(t, result.Downloaded)
		assert.Empty(t, result.Deleted)
		assert.Empty(t, result.Errors)
	})

	t.Run("Sync with special characters in filenames", func(t *testing.T) {
		specialDir := filepath.Join(t.TempDir(), "special")
		err := os.MkdirAll(specialDir, 0755)
		require.NoError(t, err)

		specialFiles := map[string]string{
			"file with spaces.txt":      "content with spaces",
			"file-with-dashes.txt":      "content with dashes",
			"file_with_underscores.txt": "content with underscores",
			"file.with.dots.txt":        "content with dots",
			"file123numbers.txt":        "content with numbers",
		}

		for filename, content := range specialFiles {
			fullPath := filepath.Join(specialDir, filename)
			err := os.WriteFile(fullPath, []byte(content), 0644)
			require.NoError(t, err)
		}

		source = specialDir
		destination = fmt.Sprintf("s3://%s/special-chars/", bucketName)
		bucket = bucketName
		recursive = true
		quiet = true

		result, err := syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)

		assert.Len(t, result.Uploaded, len(specialFiles))
		assert.Empty(t, result.Errors)

		for filename, expectedContent := range specialFiles {
			obj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("special-chars/" + filename),
			})
			require.NoError(t, err)
			defer func() {
				if closeErr := obj.Body.Close(); closeErr != nil {
					t.Logf("Warning: failed to close response body: %v", closeErr)
				}
			}()

			content := make([]byte, len(expectedContent)+10)
			n, err := obj.Body.Read(content)
			if err != nil && err.Error() != "EOF" {
				require.NoError(t, err)
			}

			assert.Equal(t, expectedContent, string(content[:n]))
		}
	})
}

func TestSyncDryRun(t *testing.T) {
	ctx := context.Background()
	bucketName := "dry-run-test-bucket"

	restore := preserveGlobalVars()
	defer restore()

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()

	testFiles := map[string]string{
		"file1.txt":        "Content 1",
		"file2.txt":        "Content 2",
		"subdir/file3.txt": "Content 3",
	}

	for relPath, content := range testFiles {
		fullPath := filepath.Join(tempDir, relPath)
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		require.NoError(t, err)
		err = os.WriteFile(fullPath, []byte(content), 0644)
		require.NoError(t, err)
	}

	t.Run("Dry run should not upload files", func(t *testing.T) {
		source = tempDir
		destination = fmt.Sprintf("s3://%s/dry-run-test/", bucketName)
		bucket = bucketName
		recursive = true
		quiet = true
		dryRun = true

		_, err := syncLocalToS3(ctx, s3Client)
		require.NoError(t, err)

		for relPath := range testFiles {
			_, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("dry-run-test/" + relPath),
			})
			assert.Error(t, err, "File %s should not exist in S3 during dry run", relPath)
		}
	})
}
