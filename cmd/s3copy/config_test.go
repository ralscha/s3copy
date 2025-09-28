package main

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	t.Run("environment variable configuration", func(t *testing.T) {
		os.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
		os.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
		os.Setenv("AWS_REGION", "us-west-2")
		defer func() {
			os.Unsetenv("AWS_ACCESS_KEY_ID")
			os.Unsetenv("AWS_SECRET_ACCESS_KEY")
			os.Unsetenv("AWS_REGION")
		}()

		config = Config{}
		resetS3Client()

		ctx := context.Background()
		client, _ := getS3Client(ctx)
		assert.NotNil(t, client)
	})

	t.Run("config fields", func(t *testing.T) {
		cfg := Config{
			AccessKey: "test-key",
			SecretKey: "test-secret",
			Region:    "us-east-1",
			Endpoint:  "http://localhost:9000",
		}

		assert.Equal(t, "test-key", cfg.AccessKey)
		assert.Equal(t, "test-secret", cfg.SecretKey)
		assert.Equal(t, "us-east-1", cfg.Region)
		assert.Equal(t, "http://localhost:9000", cfg.Endpoint)
	})
}
