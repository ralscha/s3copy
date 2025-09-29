package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// calculateFileMD5 calculates the MD5 checksum of a file
func calculateFileMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()

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

// runWorkerPool executes tasks using a worker pool pattern
func runWorkerPool[T any](tasks []T, maxWorkers int, worker func(T) error) error {
	if len(tasks) == 0 {
		return nil
	}

	bufferSize := min(maxWorkers*2, len(tasks))
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
