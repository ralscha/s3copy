package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
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

// safeLocalPath converts an S3-relative path to a path below root. S3 keys are
// untrusted input: without this check, a key containing ".." (or a Windows
// drive path) could write outside the requested download directory.
func safeLocalPath(root, relativePath string) (string, error) {
	cleaned, err := safeRelativePath(relativePath)
	if err != nil {
		return "", err
	}

	localRelative := filepath.FromSlash(cleaned)
	if filepath.IsAbs(localRelative) || filepath.VolumeName(localRelative) != "" {
		return "", fmt.Errorf("unsafe absolute path %q", relativePath)
	}

	joined := filepath.Join(root, localRelative)
	rel, err := filepath.Rel(root, joined)
	if err != nil {
		return "", fmt.Errorf("resolve local path %q: %w", relativePath, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe path traversal in %q", relativePath)
	}

	resolvedRoot, err := resolveExistingPath(root)
	if err != nil {
		return "", fmt.Errorf("resolve download root: %w", err)
	}
	resolvedTarget, err := resolveExistingPath(joined)
	if err != nil {
		return "", fmt.Errorf("resolve local path %q: %w", relativePath, err)
	}
	resolvedRel, err := filepath.Rel(resolvedRoot, resolvedTarget)
	if err != nil || resolvedRel == ".." || strings.HasPrefix(resolvedRel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe symlink traversal in %q", relativePath)
	}

	return joined, nil
}

// resolveExistingPath resolves symlinks in the existing portion of a path and
// appends any not-yet-created suffix. This catches a destination child that is
// already a symlink outside the chosen download root.
func resolveExistingPath(filePath string) (string, error) {
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return "", err
	}

	current := filepath.Clean(absPath)
	var missing []string
	for {
		_, statErr := os.Lstat(current)
		if statErr == nil {
			break
		}
		if !os.IsNotExist(statErr) {
			return "", statErr
		}

		parent := filepath.Dir(current)
		if parent == current {
			return "", statErr
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}

	resolved, err := filepath.EvalSymlinks(current)
	if err != nil {
		return "", err
	}
	for i := len(missing) - 1; i >= 0; i-- {
		resolved = filepath.Join(resolved, missing[i])
	}
	return filepath.Clean(resolved), nil
}

func safeRelativePath(relativePath string) (string, error) {
	if strings.Contains(relativePath, "\\") {
		return "", fmt.Errorf("unsafe non-portable path %q", relativePath)
	}
	normalized := relativePath
	if normalized == "" || strings.ContainsRune(normalized, '\x00') {
		return "", fmt.Errorf("unsafe empty local path")
	}
	if strings.HasPrefix(normalized, "/") {
		return "", fmt.Errorf("unsafe absolute path %q", relativePath)
	}

	for _, segment := range strings.Split(normalized, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("unsafe non-canonical path %q", relativePath)
		}
	}

	cleaned := path.Clean(normalized)
	if cleaned == "." {
		return "", fmt.Errorf("unsafe empty local path")
	}
	if len(cleaned) >= 2 && cleaned[1] == ':' && ((cleaned[0] >= 'a' && cleaned[0] <= 'z') || (cleaned[0] >= 'A' && cleaned[0] <= 'Z')) {
		return "", fmt.Errorf("unsafe absolute path %q", relativePath)
	}
	return cleaned, nil
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
		if err := producer(workerCtx, taskChan); err != nil {
			// A canceled producer is expected only when a worker (or the parent
			// context) already canceled the shared context. An independently
			// returned context.Canceled is still a real producer error.
			if !errors.Is(err, context.Canceled) || workerCtx.Err() == nil {
				select {
				case errChan <- err:
				default:
				}
				cancel()
			}
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
	if verbose && !quiet {
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
