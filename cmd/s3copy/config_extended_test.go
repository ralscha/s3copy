package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateS3Config(t *testing.T) {
	restore := preserveGlobalVars()
	defer restore()

	ctx := context.Background()

	t.Run("create config with endpoint", func(t *testing.T) {
		config = Config{
			Endpoint:  "http://localhost:9000",
			AccessKey: "test-key",
			SecretKey: "test-secret",
			Region:    "us-east-1",
		}
		resetS3Client()

		cfg, err := createS3Config(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, cfg)
		assert.Equal(t, "us-east-1", cfg.Region)
	})

	t.Run("create config without endpoint", func(t *testing.T) {
		config = Config{
			Endpoint:  "",
			AccessKey: "test-key",
			SecretKey: "test-secret",
			Region:    "eu-west-1",
		}
		resetS3Client()

		cfg, err := createS3Config(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, cfg)
		assert.Equal(t, "eu-west-1", cfg.Region)
	})

	t.Run("create config with custom retry", func(t *testing.T) {
		config = Config{
			AccessKey: "test-key",
			SecretKey: "test-secret",
			Region:    "us-west-2",
		}
		resetS3Client()

		cfg, err := createS3Config(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, cfg)
	})
}

func TestLoadConfigFromEnv(t *testing.T) {
	for _, key := range []string{
		"S3COPY_ENDPOINT",
		"S3COPY_ACCESS_KEY",
		"S3COPY_SECRET_KEY",
		"S3COPY_SESSION_TOKEN",
		"S3COPY_REGION",
		"S3COPY_USE_PATH_STYLE",
	} {
		t.Setenv(key, "")
	}

	t.Run("default credential chain", func(t *testing.T) {
		loaded, err := loadConfigFromEnv()
		require.NoError(t, err)
		assert.Empty(t, loaded.AccessKey)
		assert.Empty(t, loaded.SecretKey)
		assert.Equal(t, "us-east-1", loaded.Region)
	})

	t.Run("static credentials with session token", func(t *testing.T) {
		t.Setenv("S3COPY_ACCESS_KEY", "access")
		t.Setenv("S3COPY_SECRET_KEY", "secret")
		t.Setenv("S3COPY_SESSION_TOKEN", "token")
		t.Setenv("S3COPY_USE_PATH_STYLE", "TRUE")

		loaded, err := loadConfigFromEnv()
		require.NoError(t, err)
		assert.Equal(t, "token", loaded.SessionToken)
		assert.True(t, loaded.UsePathStyle)
	})

	t.Run("partial static credentials", func(t *testing.T) {
		t.Setenv("S3COPY_ACCESS_KEY", "access")
		t.Setenv("S3COPY_SECRET_KEY", "")
		t.Setenv("S3COPY_SESSION_TOKEN", "")
		_, err := loadConfigFromEnv()
		assert.Error(t, err)
	})

	t.Run("invalid path style", func(t *testing.T) {
		t.Setenv("S3COPY_ACCESS_KEY", "")
		t.Setenv("S3COPY_USE_PATH_STYLE", "sometimes")
		_, err := loadConfigFromEnv()
		assert.Error(t, err)
	})
}

func TestGetS3Client(t *testing.T) {
	restore := preserveGlobalVars()
	defer restore()

	ctx := context.Background()

	t.Run("create new client", func(t *testing.T) {
		config = Config{
			AccessKey:    "test-key",
			SecretKey:    "test-secret",
			Region:       "us-east-1",
			UsePathStyle: false,
		}
		resetS3Client()

		client, err := getS3Client(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})

	t.Run("reuse existing client", func(t *testing.T) {
		config = Config{
			AccessKey:    "test-key",
			SecretKey:    "test-secret",
			Region:       "us-east-1",
			UsePathStyle: false,
		}
		resetS3Client()

		client1, err := getS3Client(ctx)
		require.NoError(t, err)

		client2, err := getS3Client(ctx)
		require.NoError(t, err)

		assert.Equal(t, client1, client2)
	})

	t.Run("create client with path style", func(t *testing.T) {
		config = Config{
			AccessKey:    "test-key",
			SecretKey:    "test-secret",
			Region:       "us-east-1",
			UsePathStyle: true,
		}
		resetS3Client()

		client, err := getS3Client(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})
}

func TestResetS3Client(t *testing.T) {
	ctx := context.Background()

	t.Run("reset client", func(t *testing.T) {
		config = Config{
			AccessKey: "test-key",
			SecretKey: "test-secret",
			Region:    "us-east-1",
		}
		resetS3Client()

		_, _ = getS3Client(ctx)
		assert.NotNil(t, s3ClientInstance)

		resetS3Client()
		assert.Nil(t, s3ClientInstance)
	})
}
