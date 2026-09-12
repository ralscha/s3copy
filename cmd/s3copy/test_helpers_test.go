package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupRustFSTest(t *testing.T, ctx context.Context, bucketName string) *s3.Client {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	const (
		accessKey = "s3copy-test-access"
		secretKey = "s3copy-test-secret"
	)

	rustfsContainer, err := testcontainers.Run(ctx, "rustfs/rustfs:1.0.0-rc.5",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"RUSTFS_ACCESS_KEY":     accessKey,
			"RUSTFS_SECRET_KEY":     secretKey,
			"RUSTFS_ADDRESS":        ":9000",
			"RUSTFS_CONSOLE_ENABLE": "false",
		}),
		testcontainers.WithCmd("/data"),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/health/ready").WithPort("9000/tcp").WithStartupTimeout(2*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, rustfsContainer)
	require.NoError(t, err, "start RustFS")

	endpoint, err := rustfsContainer.PortEndpoint(ctx, "9000/tcp", "http")
	require.NoError(t, err)

	resetS3Client()

	config = Config{
		Endpoint:     endpoint,
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		Region:       "us-east-1",
		UsePathStyle: true,
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithBaseEndpoint(endpoint),
	)
	require.NoError(t, err)

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	if bucketName != "" {
		_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		require.NoError(t, err)
	}

	return s3Client
}

func captureStdout(fn func()) string {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	fn()

	closeWithLog(w, "captured stdout")
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.String()
}

func setTestConfig(src, dst, bkt string, enc, rec, qu, verb bool) {
	source = src
	destination = dst
	bucket = bkt
	encrypt = enc
	recursive = rec
	envFile = ""
	listObjects = false
	filter = ""
	listDetailed = false
	ignorePatterns = ""
	ignoreFile = ""
	maxWorkers = 5
	dryRun = false
	quiet = qu
	verbose = verb
	timeout = 0
	retries = 3
	forceOverwrite = false
	syncMode = false
	syncCompare = "checksum"
}

func preserveGlobalVars() func() {
	originalSource := source
	originalDestination := destination
	originalBucket := bucket
	originalEncrypt := encrypt
	originalRecursive := recursive
	originalEnvFile := envFile
	originalListObjects := listObjects
	originalFilter := filter
	originalListDetailed := listDetailed
	originalQuiet := quiet
	originalVerbose := verbose
	originalMaxWorkers := maxWorkers
	originalDryRun := dryRun
	originalTimeout := timeout
	originalRetries := retries
	originalForceOverwrite := forceOverwrite
	originalSyncMode := syncMode
	originalIgnorePatterns := ignorePatterns
	originalIgnoreFile := ignoreFile
	originalIgnoreMatcher := ignoreMatcher
	originalSyncCompare := syncCompare
	originalPassword := password

	return func() {
		source = originalSource
		destination = originalDestination
		bucket = originalBucket
		encrypt = originalEncrypt
		recursive = originalRecursive
		envFile = originalEnvFile
		listObjects = originalListObjects
		filter = originalFilter
		listDetailed = originalListDetailed
		quiet = originalQuiet
		verbose = originalVerbose
		maxWorkers = originalMaxWorkers
		dryRun = originalDryRun
		timeout = originalTimeout
		retries = originalRetries
		forceOverwrite = originalForceOverwrite
		syncMode = originalSyncMode
		ignorePatterns = originalIgnorePatterns
		ignoreFile = originalIgnoreFile
		ignoreMatcher = originalIgnoreMatcher
		syncCompare = originalSyncCompare
		password = originalPassword
	}
}
