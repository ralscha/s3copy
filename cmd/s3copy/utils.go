package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Constants for configurable parameters
const (
	// DefaultEncryptionChunkSize is the default chunk size for encryption (1MB)
	DefaultEncryptionChunkSize = 1024 * 1024
	// DefaultWorkerPoolBufferMultiplier determines the buffer size for worker pool
	DefaultWorkerPoolBufferMultiplier = 2
)

// calculateFileMD5 calculates the MD5 checksum of a file
func calculateFileMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer closeWithLog(file, filePath)

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// retryOperation executes an operation with retry logic
func retryOperation(operation func() error, operationType string, maxAttempts int) error {
	var lastErr error
	for attempts := range maxAttempts {
		lastErr = operation()
		if lastErr == nil {
			return nil
		}
		if attempts < maxAttempts-1 {
			logVerbose("%s attempt %d failed, retrying...\n", operationType, attempts+1)
		}
	}
	return fmt.Errorf("failed to %s after %d attempts: %v", strings.ToLower(operationType), maxAttempts, lastErr)
}

// runWorkerPool executes tasks using a worker pool pattern with context support
func runWorkerPool[T any](tasks []T, maxWorkers int, worker func(T) error) error {
	if len(tasks) == 0 {
		return nil
	}

	bufferSize := min(maxWorkers*DefaultWorkerPoolBufferMultiplier, len(tasks))
	taskChan := make(chan T, bufferSize)
	errChan := make(chan error, 1) // Only need to capture first error
	var wg sync.WaitGroup

	numWorkers := min(maxWorkers, len(tasks))

	for range numWorkers {
		wg.Go(func() {
			for task := range taskChan {
				if err := worker(task); err != nil {
					select {
					case errChan <- err:
					default:
					}
					return
				}
			}
		})
	}

	for _, task := range tasks {
		taskChan <- task
	}
	close(taskChan)

	wg.Wait()
	close(errChan)

	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

func logInfo(format string, args ...any) {
	if !quiet {
		fmt.Printf(format, args...)
	}
}

func logVerbose(format string, args ...any) {
	if verbose {
		fmt.Printf(format, args...)
	}
}

// closeWithLog closes a resource and logs any error
func closeWithLog(closer io.Closer, resourceName string) {
	if err := closer.Close(); err != nil {
		logVerbose("Warning: failed to close %s: %v\n", resourceName, err)
	}
}

// performS3Upload performs an S3 upload operation with optional encryption
func performS3Upload(ctx context.Context, uploader *manager.Uploader, bucket, key string, reader io.Reader, encrypt bool, metadata map[string]string) error {
	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   reader,
	}

	if len(metadata) > 0 {
		uploadInput.Metadata = metadata
	}

	return retryOperation(func() error {
		_, err := uploader.Upload(ctx, uploadInput)
		return err
	}, "Upload", retries)
}

// performS3Download performs an S3 download operation with optional decryption
func performS3Download(ctx context.Context, downloader *manager.Downloader, bucket, key string, writer io.WriterAt) error {
	return retryOperation(func() error {
		_, err := downloader.Download(ctx, writer, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		return err
	}, "Download", retries)
}

// setupEncryptionPipe creates a pipe for encryption and returns reader, writer, and error channel
func setupEncryptionPipe(sourceReader io.Reader) (io.Reader, *io.PipeWriter, chan error) {
	pipeReader, pipeWriter := io.Pipe()
	errChan := make(chan error, 1)

	go func() {
		defer closeWithLog(pipeWriter, "encryption pipe writer")
		errChan <- encryptStream(pipeWriter, sourceReader)
	}()

	return pipeReader, pipeWriter, errChan
}

// compareFileChecksums compares local file checksum with S3 object checksum
func compareFileChecksums(ctx context.Context, s3Client *s3.Client, bucket, s3Key, localMD5 string) (bool, error) {
	exists, etag, metadata, err := checkS3ObjectExists(ctx, s3Client, bucket, s3Key)
	if err != nil {
		return false, fmt.Errorf("could not check S3 object: %v", err)
	}

	if !exists {
		return false, nil
	}

	if etag == localMD5 {
		logInfo("Skipping %s (already exists with same checksum via ETag)\n", s3Key)
		return true, nil
	}

	if storedMD5, exists := metadata["local-md5"]; exists {
		if storedMD5 == localMD5 {
			logInfo("Skipping %s (already exists with same checksum via metadata)\n", s3Key)
			return true, nil
		}
		logVerbose("Object exists but checksum differs (local: %s, metadata: %s, etag: %s)\n", localMD5, storedMD5, etag)
	} else {
		logVerbose("Object exists but no local MD5 in metadata, will upload (local: %s, etag: %s)\n", localMD5, etag)
	}

	return false, nil
}
