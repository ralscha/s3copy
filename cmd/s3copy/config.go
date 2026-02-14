package main

import (
	"context"
	"fmt"
	"os"
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
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, "")),
		awsconfig.WithRegion(config.Region),
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.AddWithMaxAttempts(retry.NewStandard(), retries)
		}),
	}

	if config.Endpoint != "" {
		configOptions = append(configOptions, awsconfig.WithBaseEndpoint(config.Endpoint))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, configOptions...)

	return cfg, err
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
