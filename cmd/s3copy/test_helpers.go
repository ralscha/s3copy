package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func setupMinIOTest(t *testing.T, ctx context.Context, bucketName string) (*s3.Client, func()) {
	t.Helper()

	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2025-09-07T16-13-09Z")
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("skipping MinIO integration test: Docker/Testcontainers unavailable: %v", err)
		}
		require.NoError(t, err)
	}

	cleanup := func() {
		testcontainers.CleanupContainer(t, minioContainer)
	}

	endpoint, err := minioContainer.Endpoint(ctx, "")
	require.NoError(t, err)

	if !strings.HasPrefix(endpoint, "http://") {
		endpoint = "http://" + endpoint
	}

	resetS3Client()

	config = Config{
		Endpoint:     endpoint,
		AccessKey:    "minioadmin",
		SecretKey:    "minioadmin",
		Region:       "us-east-1",
		UsePathStyle: true,
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
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

	return s3Client, cleanup
}

func isDockerUnavailable(err error) bool {
	msg := strings.ToLower(err.Error())
	unavailableMessages := []string{
		"cannot connect to the docker daemon",
		"docker is not available",
		"docker daemon",
		"failed to create docker provider",
		"rootless docker is not supported",
	}

	for _, unavailableMessage := range unavailableMessages {
		if strings.Contains(msg, unavailableMessage) {
			return true
		}
	}

	return false
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
