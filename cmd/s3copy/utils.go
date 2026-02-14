package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

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

// runWorkerPool executes tasks using a worker pool pattern with context support
func runWorkerPool[T any](ctx context.Context, tasks []T, maxWorkers int, worker func(context.Context, T) error) error {
	if len(tasks) == 0 {
		return nil
	}

	if maxWorkers < 1 {
		return fmt.Errorf("max workers must be at least 1, got %d", maxWorkers)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	bufferSize := min(maxWorkers*DefaultWorkerPoolBufferMultiplier, len(tasks))
	taskChan := make(chan T, bufferSize)
	errChan := make(chan error, 1) // Only need to capture first error
	var wg sync.WaitGroup

	numWorkers := min(maxWorkers, len(tasks))

	for range numWorkers {
		wg.Go(func() {
			for {
				select {
				case <-workerCtx.Done():
					return
				case task, ok := <-taskChan:
					if !ok {
						return
					}
					if err := worker(workerCtx, task); err != nil {
						if errors.Is(err, context.Canceled) && workerCtx.Err() != nil {
							return
						}
						select {
						case errChan <- err:
						default:
						}
						cancel()
						return
					}
				}
			}
		})
	}

sendLoop:
	for _, task := range tasks {
		select {
		case <-workerCtx.Done():
			break sendLoop
		case taskChan <- task:
		}
	}
	close(taskChan)

	wg.Wait()
	close(errChan)

	select {
	case err := <-errChan:
		return err
	default:
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
}

// runWorkerPoolStream executes streamed tasks with a worker pool to avoid building large task slices in memory.
func runWorkerPoolStream[T any](ctx context.Context, maxWorkers int, worker func(context.Context, T) error, producer func(context.Context, chan<- T) error) error {
	if maxWorkers < 1 {
		return fmt.Errorf("max workers must be at least 1, got %d", maxWorkers)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	bufferSize := maxWorkers * DefaultWorkerPoolBufferMultiplier
	taskChan := make(chan T, bufferSize)
	errChan := make(chan error, 2)
	var wg sync.WaitGroup

	for range maxWorkers {
		wg.Go(func() {
			for {
				select {
				case <-workerCtx.Done():
					return
				case task, ok := <-taskChan:
					if !ok {
						return
					}
					if err := worker(workerCtx, task); err != nil {
						if errors.Is(err, context.Canceled) && workerCtx.Err() != nil {
							return
						}
						select {
						case errChan <- err:
						default:
						}
						cancel()
						return
					}
				}
			}
		})
	}

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		defer close(taskChan)
		if err := producer(workerCtx, taskChan); err != nil && !errors.Is(err, context.Canceled) {
			select {
			case errChan <- err:
			default:
			}
			cancel()
		}
	}()

	wg.Wait()
	<-producerDone
	close(errChan)

	select {
	case err := <-errChan:
		return err
	default:
		if err := ctx.Err(); err != nil {
			return err
		}
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
