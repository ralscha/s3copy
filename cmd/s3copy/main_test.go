package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunCopyValidation(t *testing.T) {
	restore := preserveGlobalVars()
	defer restore()

	t.Run("missing access key", func(t *testing.T) {
		_ = os.Setenv("S3COPY_ACCESS_KEY", "")
		_ = os.Setenv("S3COPY_SECRET_KEY", "test-secret")
		defer func() {
			_ = os.Unsetenv("S3COPY_ACCESS_KEY")
			_ = os.Unsetenv("S3COPY_SECRET_KEY")
		}()

		source = "/tmp/test"
		destination = "s3://bucket/key"
		bucket = "bucket"
		listObjects = false

		err := runCopy()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing required environment variables")
	})

	t.Run("missing secret key", func(t *testing.T) {
		_ = os.Setenv("S3COPY_ACCESS_KEY", "test-key")
		_ = os.Setenv("S3COPY_SECRET_KEY", "")
		defer func() {
			_ = os.Unsetenv("S3COPY_ACCESS_KEY")
			_ = os.Unsetenv("S3COPY_SECRET_KEY")
		}()

		source = "/tmp/test"
		destination = "s3://bucket/key"
		bucket = "bucket"
		listObjects = false

		err := runCopy()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing required environment variables")
	})

	t.Run("both source and destination are local", func(t *testing.T) {
		_ = os.Setenv("S3COPY_ACCESS_KEY", "test-key")
		_ = os.Setenv("S3COPY_SECRET_KEY", "test-secret")
		defer func() {
			_ = os.Unsetenv("S3COPY_ACCESS_KEY")
			_ = os.Unsetenv("S3COPY_SECRET_KEY")
		}()

		source = "/tmp/source"
		destination = "/tmp/dest"
		bucket = "bucket"
		listObjects = false
		syncMode = false

		err := runCopy()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least one of source or destination must be S3")
	})

	t.Run("both source and destination are S3", func(t *testing.T) {
		_ = os.Setenv("S3COPY_ACCESS_KEY", "test-key")
		_ = os.Setenv("S3COPY_SECRET_KEY", "test-secret")
		defer func() {
			_ = os.Unsetenv("S3COPY_ACCESS_KEY")
			_ = os.Unsetenv("S3COPY_SECRET_KEY")
		}()

		source = "s3://bucket1/key1"
		destination = "s3://bucket2/key2"
		bucket = "bucket1"
		listObjects = false
		syncMode = false

		err := runCopy()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "S3 to S3 copy is not supported")
	})

	t.Run("invalid ignore file", func(t *testing.T) {
		_ = os.Setenv("S3COPY_ACCESS_KEY", "test-key")
		_ = os.Setenv("S3COPY_SECRET_KEY", "test-secret")
		defer func() {
			_ = os.Unsetenv("S3COPY_ACCESS_KEY")
			_ = os.Unsetenv("S3COPY_SECRET_KEY")
		}()

		source = "/tmp/test"
		destination = "s3://bucket/key"
		bucket = "bucket"
		listObjects = false
		ignoreFile = "/nonexistent/ignore/file.txt"

		err := runCopy()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error initializing ignore patterns")
	})
}

func TestRunCopyWithEnvFile(t *testing.T) {
	restore := preserveGlobalVars()
	defer restore()

	tempDir := t.TempDir()
	envFilePath := filepath.Join(tempDir, "test.env")

	envContent := `S3COPY_ACCESS_KEY=test-access-key
S3COPY_SECRET_KEY=test-secret-key
S3COPY_REGION=us-west-2
S3COPY_ENDPOINT=http://localhost:9000
S3COPY_USE_PATH_STYLE=true
`
	err := os.WriteFile(envFilePath, []byte(envContent), 0644)
	require.NoError(t, err)

	t.Run("load valid env file", func(t *testing.T) {
		envFile = envFilePath
		source = "s3://bucket/key"
		destination = "/tmp/dest"
		bucket = "bucket"
		listObjects = false

		// This will fail at downloading, but we're testing env loading
		err := runCopy()
		// Error is expected (can't connect to S3), but config should be loaded
		assert.Error(t, err)
		assert.Equal(t, "test-access-key", config.AccessKey)
		assert.Equal(t, "test-secret-key", config.SecretKey)
		assert.Equal(t, "us-west-2", config.Region)
		assert.Equal(t, "http://localhost:9000", config.Endpoint)
		assert.True(t, config.UsePathStyle)
	})
}

func TestRunCopyTimeout(t *testing.T) {
	restore := preserveGlobalVars()
	defer restore()

	_ = os.Setenv("S3COPY_ACCESS_KEY", "test-key")
	_ = os.Setenv("S3COPY_SECRET_KEY", "test-secret")
	defer func() {
		_ = os.Unsetenv("S3COPY_ACCESS_KEY")
		_ = os.Unsetenv("S3COPY_SECRET_KEY")
	}()

	t.Run("with timeout set", func(t *testing.T) {
		source = "s3://bucket/key"
		destination = "/tmp/dest"
		bucket = "bucket"
		listObjects = false
		timeout = 1 // 1 second timeout

		err := runCopy()
		// Will fail but timeout context should be created
		assert.Error(t, err)
	})
}

func TestGetEnvOrDefault(t *testing.T) {
	t.Run("environment variable exists", func(t *testing.T) {
		_ = os.Setenv("TEST_VAR", "test-value")
		defer func() { _ = os.Unsetenv("TEST_VAR") }()

		result := getEnvOrDefault("TEST_VAR", "default-value")
		assert.Equal(t, "test-value", result)
	})

	t.Run("environment variable does not exist", func(t *testing.T) {
		result := getEnvOrDefault("NON_EXISTENT_VAR", "default-value")
		assert.Equal(t, "default-value", result)
	})

	t.Run("environment variable is empty", func(t *testing.T) {
		_ = os.Setenv("EMPTY_VAR", "")
		defer func() { _ = os.Unsetenv("EMPTY_VAR") }()

		result := getEnvOrDefault("EMPTY_VAR", "default-value")
		assert.Equal(t, "default-value", result)
	})
}
