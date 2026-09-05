package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/term"
)

type Config struct {
	Endpoint     string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Region       string
	UsePathStyle bool
}

var (
	config           Config
	s3ClientInstance *s3.Client
	s3ClientMutex    sync.Mutex
)

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getPasswordFromUser() (string, error) {
	fmt.Print("Enter encryption password: ")
	password, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println()
	if err != nil {
		return "", err
	}
	return string(password), nil
}

func createS3Config(ctx context.Context) (aws.Config, error) {
	configOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(config.Region),
		awsconfig.WithRetryer(func() aws.Retryer {
			// The AWS SDK counts the initial request as an attempt; the CLI flag
			// counts retries after that initial request.
			return retry.AddWithMaxAttempts(retry.NewStandard(), retries+1)
		}),
	}
	if (config.AccessKey == "") != (config.SecretKey == "") || (config.SessionToken != "" && config.AccessKey == "") {
		return aws.Config{}, fmt.Errorf("S3COPY_ACCESS_KEY and S3COPY_SECRET_KEY must be set together")
	}
	if config.AccessKey != "" {
		configOptions = append(configOptions, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, config.SessionToken),
		))
	}

	if config.Endpoint != "" {
		configOptions = append(configOptions, awsconfig.WithBaseEndpoint(config.Endpoint))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, configOptions...)

	return cfg, err
}

func loadConfigFromEnv() (Config, error) {
	usePathStyle := false
	if value := os.Getenv("S3COPY_USE_PATH_STYLE"); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, fmt.Errorf("invalid S3COPY_USE_PATH_STYLE value %q: %w", value, err)
		}
		usePathStyle = parsed
	}

	loaded := Config{
		Endpoint:     getEnvOrDefault("S3COPY_ENDPOINT", ""),
		AccessKey:    getEnvOrDefault("S3COPY_ACCESS_KEY", ""),
		SecretKey:    getEnvOrDefault("S3COPY_SECRET_KEY", ""),
		SessionToken: getEnvOrDefault("S3COPY_SESSION_TOKEN", ""),
		Region:       getEnvOrDefault("S3COPY_REGION", "us-east-1"),
		UsePathStyle: usePathStyle,
	}
	if (loaded.AccessKey == "") != (loaded.SecretKey == "") || (loaded.SessionToken != "" && loaded.AccessKey == "") {
		return Config{}, fmt.Errorf("S3COPY_ACCESS_KEY and S3COPY_SECRET_KEY must be set together")
	}
	return loaded, nil
}

func getS3Client(ctx context.Context) (*s3.Client, error) {
	s3ClientMutex.Lock()
	defer s3ClientMutex.Unlock()

	if s3ClientInstance != nil {
		return s3ClientInstance, nil
	}

	cfg, err := createS3Config(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 config: %w", err)
	}

	clientOptions := []func(*s3.Options){}
	if config.UsePathStyle {
		clientOptions = append(clientOptions, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	s3ClientInstance = s3.NewFromConfig(cfg, clientOptions...)
	return s3ClientInstance, nil
}

// resetS3Client resets the singleton S3 client instance
// For testing purposes
func resetS3Client() {
	s3ClientMutex.Lock()
	defer s3ClientMutex.Unlock()
	s3ClientInstance = nil
}
