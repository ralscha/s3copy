package main

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{1099511627776, "1.0 TB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatBytes(tt.bytes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTruncateString(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		expected string
	}{
		{"short", 10, "short"},
		{"verylongstring", 10, "verylon..."},
		{"exact", 5, "exact"},
		{"toolong", 5, "to..."},
		{"a", 3, "a"},
		{"abc", 3, "abc"},
		{"abcd", 3, "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := truncateString(tt.input, tt.maxLen)
			assert.Equal(t, tt.expected, result)
			assert.LessOrEqual(t, len(result), tt.maxLen)
		})
	}
}

func TestCalculateFileMD5(t *testing.T) {
	t.Run("CalculateMD5ForFile", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test-md5-*.txt")
		require.NoError(t, err)
		defer func() { _ = os.Remove(tmpFile.Name()) }()

		testContent := "Hello, World!\nThis is a test file.\n"
		_, err = tmpFile.WriteString(testContent)
		require.NoError(t, err)
		closeWithLog(tmpFile, "test file")

		hash, err := calculateFileMD5(tmpFile.Name())
		require.NoError(t, err)
		assert.NotEmpty(t, hash)
		assert.Len(t, hash, 32)
	})

	t.Run("CalculateMD5ForNonExistentFile", func(t *testing.T) {
		_, err := calculateFileMD5("/non/existent/file.txt")
		assert.Error(t, err)
	})
}

func TestLogFunctions(t *testing.T) {
	originalQuiet := quiet
	originalVerbose := verbose
	defer func() {
		quiet = originalQuiet
		verbose = originalVerbose
	}()

	t.Run("logInfo when not quiet", func(t *testing.T) {
		quiet = false
		output := captureStdout(func() {
			logInfo("test message %s", "arg")
		})
		assert.Contains(t, output, "test message arg")
	})

	t.Run("logInfo when quiet", func(t *testing.T) {
		quiet = true
		output := captureStdout(func() {
			logInfo("should not print")
		})
		assert.Empty(t, output)
	})

	t.Run("logVerbose when verbose", func(t *testing.T) {
		verbose = true
		output := captureStdout(func() {
			logVerbose("verbose message %d", 42)
		})
		assert.Contains(t, output, "verbose message 42")
	})

	t.Run("logVerbose when not verbose", func(t *testing.T) {
		verbose = false
		output := captureStdout(func() {
			logVerbose("should not print")
		})
		assert.Empty(t, output)
	})
}

func TestRunWorkerPool(t *testing.T) {
	t.Run("invalid worker count", func(t *testing.T) {
		err := runWorkerPool(context.Background(), []int{1}, 0, func(ctx context.Context, task int) error {
			return nil
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max workers must be at least 1")
	})

	t.Run("respects canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := runWorkerPool(ctx, []int{1, 2, 3}, 2, func(ctx context.Context, task int) error {
			return nil
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("returns first worker error", func(t *testing.T) {
		expectedErr := errors.New("boom")

		err := runWorkerPool(context.Background(), []int{1, 2, 3}, 2, func(ctx context.Context, task int) error {
			if task == 2 {
				return expectedErr
			}
			return nil
		})

		require.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("returns canceled when context canceled mid-flight", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		err := runWorkerPool(ctx, []int{1}, 1, func(ctx context.Context, task int) error {
			<-ctx.Done()
			return ctx.Err()
		})

		require.Error(t, err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded))
	})
}

func TestRunWorkerPoolStream(t *testing.T) {
	t.Run("returns producer error", func(t *testing.T) {
		expectedErr := errors.New("producer failed")

		err := runWorkerPoolStream(context.Background(), 2, func(ctx context.Context, task int) error {
			return nil
		}, func(ctx context.Context, taskChan chan<- int) error {
			return expectedErr
		})

		require.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("processes streamed tasks", func(t *testing.T) {
		var (
			mu        sync.Mutex
			processed []int
		)

		err := runWorkerPoolStream(context.Background(), 2, func(ctx context.Context, task int) error {
			mu.Lock()
			processed = append(processed, task)
			mu.Unlock()
			return nil
		}, func(ctx context.Context, taskChan chan<- int) error {
			for i := 1; i <= 5; i++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case taskChan <- i:
				}
			}
			return nil
		})

		require.NoError(t, err)
		assert.Len(t, processed, 5)
	})
}
