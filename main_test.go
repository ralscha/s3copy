package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ignore "github.com/sabhiram/go-gitignore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func setupMinIOTest(t *testing.T, ctx context.Context, bucketName string) (*s3.Client, func()) {
	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2025-09-07T16-13-09Z")
	require.NoError(t, err)

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

func captureStdout(fn func()) string {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
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
}

func preserveGlobalVars() func() {
	originalSource := source
	originalDestination := destination
	originalBucket := bucket
	originalEncrypt := encrypt
	originalRecursive := recursive
	originalQuiet := quiet
	originalVerbose := verbose
	originalIgnorePatterns := ignorePatterns
	originalIgnoreFile := ignoreFile
	originalIgnoreMatcher := ignoreMatcher

	return func() {
		source = originalSource
		destination = originalDestination
		bucket = originalBucket
		encrypt = originalEncrypt
		recursive = originalRecursive
		quiet = originalQuiet
		verbose = originalVerbose
		ignorePatterns = originalIgnorePatterns
		ignoreFile = originalIgnoreFile
		ignoreMatcher = originalIgnoreMatcher
	}
}

func TestNonceManager(t *testing.T) {
	t.Run("NewNonceManager", func(t *testing.T) {
		nm, err := NewNonceManager()
		require.NoError(t, err)
		assert.NotNil(t, nm)
		assert.NotNil(t, nm.baseNonce)
		assert.Equal(t, uint64(0), nm.counter)
	})

	t.Run("NextNonce_Sequential", func(t *testing.T) {
		nm, err := NewNonceManager()
		require.NoError(t, err)

		nonce1 := nm.NextNonce()
		assert.Equal(t, uint64(1), nm.counter)
		assert.Len(t, nonce1, 12) // ChaCha20 nonce size

		nonce2 := nm.NextNonce()
		assert.Equal(t, uint64(2), nm.counter)
		assert.Len(t, nonce2, 12)

		assert.NotEqual(t, nonce1, nonce2)
	})

	t.Run("GetBaseNonce", func(t *testing.T) {
		nm, err := NewNonceManager()
		require.NoError(t, err)

		baseNonce := nm.GetBaseNonce()
		assert.Len(t, baseNonce, 12)
		assert.Equal(t, nm.baseNonce, baseNonce)
	})

	t.Run("Reset", func(t *testing.T) {
		nm, err := NewNonceManager()
		require.NoError(t, err)

		_ = nm.NextNonce()
		_ = nm.NextNonce()
		assert.Equal(t, uint64(2), nm.counter)

		nm.Reset()
		assert.Equal(t, uint64(0), nm.counter)
	})
}

func TestParseS3Path(t *testing.T) {
	tests := []struct {
		name           string
		s3Path         string
		providedBucket string
		isDir          bool
		localPath      string
		expectedBucket string
		expectedKey    string
		expectError    bool
	}{
		{
			name:           "full s3 path",
			s3Path:         "s3://mybucket/path/to/file.txt",
			providedBucket: "",
			isDir:          false,
			localPath:      "/tmp/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "path/to/file.txt",
			expectError:    false,
		},
		{
			name:           "bucket only with provided bucket",
			s3Path:         "s3://mybucket",
			providedBucket: "",
			isDir:          false,
			localPath:      "/tmp/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "file.txt",
			expectError:    false,
		},
		{
			name:           "with provided bucket",
			s3Path:         "s3://mybucket/path",
			providedBucket: "otherbucket",
			isDir:          false,
			localPath:      "/tmp/file.txt",
			expectedBucket: "otherbucket",
			expectedKey:    "mybucket/path",
			expectError:    false,
		},
		{
			name:           "directory path",
			s3Path:         "s3://mybucket/path/",
			providedBucket: "",
			isDir:          true,
			localPath:      "/tmp/dir",
			expectedBucket: "mybucket",
			expectedKey:    "path/",
			expectError:    false,
		},
		{
			name:           "directory without bucket",
			s3Path:         "s3://",
			providedBucket: "",
			isDir:          true,
			localPath:      "/tmp/dir",
			expectError:    true,
		},
		{
			name:           "bucket with trailing slash, no provided bucket, not dir",
			s3Path:         "s3://mybucket/",
			providedBucket: "",
			isDir:          false,
			localPath:      "/tmp/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "file.txt",
			expectError:    false,
		},
		{
			name:           "provided bucket with trailing slash",
			s3Path:         "s3://otherbucket/",
			providedBucket: "otherbucket",
			isDir:          false,
			localPath:      "/tmp/file.txt",
			expectedBucket: "otherbucket",
			expectedKey:    "file.txt",
			expectError:    false,
		},
		{
			name:           "s3 path with directory and trailing slash - single copy mode",
			s3Path:         "s3://mybucket/path/to/dir/",
			providedBucket: "",
			isDir:          false,
			localPath:      "/tmp/myfile.txt",
			expectedBucket: "mybucket",
			expectedKey:    "path/to/dir/myfile.txt",
			expectError:    false,
		},
		{
			name:           "provided bucket with directory and trailing slash - single copy mode",
			s3Path:         "s3://mybucket/some/folder/",
			providedBucket: "mybucket",
			isDir:          false,
			localPath:      "/local/path/document.pdf",
			expectedBucket: "mybucket",
			expectedKey:    "some/folder/document.pdf",
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := parseS3Path(tt.s3Path, tt.providedBucket, tt.isDir, tt.localPath)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedBucket, bucket)
				assert.Equal(t, tt.expectedKey, key)
			}
		})
	}
}

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

func TestInitializeIgnoreMatcher(t *testing.T) {
	restore := preserveGlobalVars()
	defer restore()

	t.Run("no patterns", func(t *testing.T) {
		ignorePatterns = ""
		ignoreFile = ""
		err := initializeIgnoreMatcher()
		assert.NoError(t, err)
		assert.Nil(t, ignoreMatcher)
	})

	t.Run("with patterns", func(t *testing.T) {
		ignorePatterns = "*.tmp,*.log"
		ignoreFile = ""
		err := initializeIgnoreMatcher()
		assert.NoError(t, err)
		assert.NotNil(t, ignoreMatcher)

		assert.True(t, ignoreMatcher.MatchesPath("file.tmp"))
		assert.True(t, ignoreMatcher.MatchesPath("file.log"))
		assert.False(t, ignoreMatcher.MatchesPath("file.txt"))
	})

	t.Run("with ignore file", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "ignore_test")
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		_, err = tempFile.WriteString("*.bak\n# comment\n*.old\n")
		require.NoError(t, err)
		tempFile.Close()

		ignorePatterns = ""
		ignoreFile = tempFile.Name()
		err = initializeIgnoreMatcher()
		assert.NoError(t, err)
		assert.NotNil(t, ignoreMatcher)

		assert.True(t, ignoreMatcher.MatchesPath("file.bak"))
		assert.True(t, ignoreMatcher.MatchesPath("file.old"))
		assert.False(t, ignoreMatcher.MatchesPath("file.txt"))
		assert.False(t, ignoreMatcher.MatchesPath("comment"))
	})
}

func TestReadIgnoreFile(t *testing.T) {
	t.Run("valid file", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "ignore_test")
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		content := "*.tmp\n\n# comment\n*.log\n"
		_, err = tempFile.WriteString(content)
		require.NoError(t, err)
		tempFile.Close()

		patterns, err := readIgnoreFile(tempFile.Name())
		assert.NoError(t, err)
		assert.Equal(t, []string{"*.tmp", "*.log"}, patterns)
	})

	t.Run("nonexistent file", func(t *testing.T) {
		_, err := readIgnoreFile("/nonexistent/file")
		assert.Error(t, err)
	})
}

func TestShouldIgnoreFile(t *testing.T) {
	restore := preserveGlobalVars()
	defer restore()

	t.Run("no matcher", func(t *testing.T) {
		ignoreMatcher = nil
		assert.False(t, shouldIgnoreFile("file.txt"))
	})

	t.Run("with matcher", func(t *testing.T) {
		patterns := []string{"*.tmp", "temp/*"}
		ignoreMatcher = ignore.CompileIgnoreLines(patterns...)

		source = "/tmp"

		assert.True(t, shouldIgnoreFile("/tmp/file.tmp"))
		assert.True(t, shouldIgnoreFile("temp/file.txt"))
		assert.False(t, shouldIgnoreFile("/tmp/file.txt"))
	})

	t.Run("relative path", func(t *testing.T) {
		patterns := []string{"*.tmp"}
		ignoreMatcher = ignore.CompileIgnoreLines(patterns...)
		source = "/tmp"

		assert.True(t, shouldIgnoreFile("file.tmp"))
		assert.False(t, shouldIgnoreFile("file.txt"))
	})

	t.Run("absolute path outside source", func(t *testing.T) {
		patterns := []string{"*.tmp"}
		ignoreMatcher = ignore.CompileIgnoreLines(patterns...)
		source = "/tmp"

		assert.True(t, shouldIgnoreFile("/other/file.tmp"))
		assert.False(t, shouldIgnoreFile("/other/file.txt"))
	})
}

func TestEncryptDecryptStream(t *testing.T) {
	password = "testpassword123"

	t.Run("round trip encryption", func(t *testing.T) {
		originalData := []byte("This is a test message for encryption and decryption.")
		input := bytes.NewReader(originalData)

		encrypted := &bytes.Buffer{}
		err := encryptStream(encrypted, input)
		require.NoError(t, err)

		encryptedReader := bytes.NewReader(encrypted.Bytes())
		decrypted := &bytes.Buffer{}
		err = decryptStreamFromReader(decrypted, encryptedReader)
		require.NoError(t, err)

		assert.Equal(t, originalData, decrypted.Bytes())
	})

	t.Run("wrong password", func(t *testing.T) {
		originalData := []byte("Test data")
		input := bytes.NewReader(originalData)

		encrypted := &bytes.Buffer{}
		err := encryptStream(encrypted, input)
		require.NoError(t, err)

		password = "wrongpassword"
		encryptedReader := bytes.NewReader(encrypted.Bytes())
		decrypted := &bytes.Buffer{}
		err = decryptStreamFromReader(decrypted, encryptedReader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "decryption failed")
	})

	t.Run("empty data", func(t *testing.T) {
		originalData := []byte("")
		input := bytes.NewReader(originalData)

		encrypted := &bytes.Buffer{}
		err := encryptStream(encrypted, input)
		require.NoError(t, err)

		encryptedReader := bytes.NewReader(encrypted.Bytes())
		decrypted := &bytes.Buffer{}
		err = decryptStreamFromReader(decrypted, encryptedReader)
		require.NoError(t, err)

		decryptedData := decrypted.Bytes()
		if decryptedData == nil {
			decryptedData = []byte{}
		}
		assert.Equal(t, originalData, decryptedData)
	})

	t.Run("large data", func(t *testing.T) {
		originalData := make([]byte, 2*1024*1024)
		_, err := rand.Read(originalData)
		require.NoError(t, err)

		input := bytes.NewReader(originalData)

		encrypted := &bytes.Buffer{}
		err = encryptStream(encrypted, input)
		require.NoError(t, err)

		encryptedReader := bytes.NewReader(encrypted.Bytes())
		decrypted := &bytes.Buffer{}
		err = decryptStreamFromReader(decrypted, encryptedReader)
		require.NoError(t, err)

		assert.Equal(t, originalData, decrypted.Bytes())
	})

	t.Run("encrypt write error", func(t *testing.T) {
		originalData := []byte("test data")
		input := bytes.NewReader(originalData)

		failingWriter := &failingWriter{}
		err := encryptStream(failingWriter, input)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to write")
	})

	t.Run("decrypt write error", func(t *testing.T) {
		originalData := []byte("test data")
		input := bytes.NewReader(originalData)

		encrypted := &bytes.Buffer{}
		err := encryptStream(encrypted, input)
		require.NoError(t, err)

		encryptedReader := bytes.NewReader(encrypted.Bytes())
		failingWriter := &failingWriter{}
		err = decryptStreamFromReader(failingWriter, encryptedReader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to write decrypted data")
	})

	t.Run("decrypt read error", func(t *testing.T) {
		incompleteData := []byte("incomplete")
		encryptedReader := bytes.NewReader(incompleteData)
		decrypted := &bytes.Buffer{}
		err := decryptStreamFromReader(decrypted, encryptedReader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read encryption header")
	})
}

// failingWriter is a writer that always fails
type failingWriter struct{}

func (f *failingWriter) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("write failed")
}

func TestGetEnvOrDefault(t *testing.T) {
	originalValue := os.Getenv("TEST_VAR")
	defer func() {
		if originalValue != "" {
			os.Setenv("TEST_VAR", originalValue)
		} else {
			os.Unsetenv("TEST_VAR")
		}
	}()

	t.Run("env var exists", func(t *testing.T) {
		os.Setenv("TEST_VAR", "test_value")
		result := getEnvOrDefault("TEST_VAR", "default")
		assert.Equal(t, "test_value", result)
	})

	t.Run("env var not set", func(t *testing.T) {
		os.Unsetenv("TEST_VAR")
		result := getEnvOrDefault("TEST_VAR", "default")
		assert.Equal(t, "default", result)
	})
}

func TestWriteAtWrapper(t *testing.T) {
	t.Run("sequential writes", func(t *testing.T) {
		buf := &bytes.Buffer{}
		wrapper := &writeAtWrapper{w: buf}

		data1 := []byte("hello")
		n1, err1 := wrapper.WriteAt(data1, 0)
		assert.NoError(t, err1)
		assert.Equal(t, 5, n1)

		data2 := []byte(" world")
		n2, err2 := wrapper.WriteAt(data2, 5)
		assert.NoError(t, err2)
		assert.Equal(t, 6, n2)

		assert.Equal(t, "hello world", buf.String())
	})

	t.Run("non-sequential write", func(t *testing.T) {
		buf := &bytes.Buffer{}
		wrapper := &writeAtWrapper{w: buf}

		_, err := wrapper.WriteAt([]byte("hello"), 0)
		assert.NoError(t, err)

		_, err = wrapper.WriteAt([]byte("world"), 10)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non-sequential write")
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

func TestS3UploadDownloadWithMinIO(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket"

	_, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	testData := []byte("Hello, MinIO! This is a test file for upload and download via CLI.")
	tempDir := t.TempDir()
	sourceFile := filepath.Join(tempDir, "source.txt")
	err := os.WriteFile(sourceFile, testData, 0644)
	require.NoError(t, err)

	s3Key := "test-file.txt"

	t.Run("upload", func(t *testing.T) {
		setTestConfig(sourceFile, fmt.Sprintf("s3://%s/%s", bucketName, s3Key), bucketName, false, false, false, false)
		err := uploadToS3(ctx)
		assert.NoError(t, err)
	})

	t.Run("download", func(t *testing.T) {
		destFile := filepath.Join(tempDir, "downloaded.txt")
		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, s3Key), destFile, bucketName, false, false, false, false)
		err := downloadFromS3(ctx)
		assert.NoError(t, err)

		downloadedData, err := os.ReadFile(destFile)
		assert.NoError(t, err)
		assert.Equal(t, testData, downloadedData)
	})

	t.Run("list objects", func(t *testing.T) {
		bucket = bucketName
		listObjects = true
		filter = ""
		listDetailed = false
		quiet = false
		verbose = false

		output := captureStdout(func() {
			err := listS3Objects()
			assert.NoError(t, err)
		})
		assert.Contains(t, output, s3Key)
	})
}

func TestUploadToS3Errors(t *testing.T) {
	ctx := context.Background()

	restore := preserveGlobalVars()
	defer restore()

	config = Config{
		AccessKey: "dummy",
		SecretKey: "dummy",
		Region:    "us-east-1",
	}

	t.Run("source does not exist", func(t *testing.T) {
		setTestConfig("/nonexistent/file.txt", "s3://bucket/key", "bucket", false, false, true, false)
		err := uploadToS3(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to stat source")
	})

	t.Run("directory without recursive", func(t *testing.T) {
		tempDir := t.TempDir()
		setTestConfig(tempDir, "s3://bucket/key", "bucket", false, false, true, false)
		err := uploadToS3(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "source is a directory, use -r flag")
	})
}

func TestListS3ObjectsDetailed(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	testObjects := []struct {
		key  string
		size int64
	}{
		{"file1.txt", 100},
		{"file2.txt", 200},
		{"dir/file3.txt", 300},
	}

	for _, obj := range testObjects {
		data := make([]byte, obj.size)
		_, err := rand.Read(data)
		require.NoError(t, err)

		_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(obj.key),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)
	}

	t.Run("detailed listing", func(t *testing.T) {
		bucket = bucketName
		listObjects = true
		filter = ""
		listDetailed = true
		quiet = false
		verbose = false

		output := captureStdout(func() {
			err := listS3Objects()
			assert.NoError(t, err)
		})
		assert.Contains(t, output, "file1.txt")
		assert.Contains(t, output, "file2.txt")
		assert.Contains(t, output, "dir/file3.txt")
		assert.Contains(t, output, "Storage Class")
		assert.Contains(t, output, "ETag")
	})

	t.Run("listing with filter", func(t *testing.T) {
		bucket = bucketName
		listObjects = true
		filter = "dir/"
		listDetailed = false
		quiet = false
		verbose = false

		output := captureStdout(func() {
			err := listS3Objects()
			assert.NoError(t, err)
		})
		assert.NotContains(t, output, "file1.txt")
		assert.NotContains(t, output, "file2.txt")
		assert.Contains(t, output, "dir/file3.txt")
	})
}

func TestShouldIgnoreFileAbsolutePaths(t *testing.T) {
	restore := preserveGlobalVars()
	defer restore()

	patterns := []string{"*.tmp", "temp/*"}
	ignoreMatcher = ignore.CompileIgnoreLines(patterns...)
	source = "/tmp"

	t.Run("absolute path within source", func(t *testing.T) {
		assert.True(t, shouldIgnoreFile("/tmp/file.tmp"))
		assert.True(t, shouldIgnoreFile("/tmp/temp/file.txt"))
		assert.False(t, shouldIgnoreFile("/tmp/file.txt"))
	})

	t.Run("absolute path outside source", func(t *testing.T) {
		assert.True(t, shouldIgnoreFile("/other/file.tmp"))
		assert.False(t, shouldIgnoreFile("/other/file.txt"))
	})

	t.Run("relative path", func(t *testing.T) {
		assert.True(t, shouldIgnoreFile("file.tmp"))
		assert.True(t, shouldIgnoreFile("temp/file.txt"))
		assert.False(t, shouldIgnoreFile("file.txt"))
	})
}

func TestUploadToS3Directory(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-upload-dir-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()
	subDir := filepath.Join(tempDir, "subdir")
	err := os.MkdirAll(subDir, 0755)
	require.NoError(t, err)

	files := []string{
		filepath.Join(tempDir, "file1.txt"),
		filepath.Join(tempDir, "file2.txt"),
		filepath.Join(subDir, "file3.txt"),
	}

	for _, file := range files {
		err := os.WriteFile(file, []byte("test content"), 0644)
		require.NoError(t, err)
	}

	t.Run("upload directory", func(t *testing.T) {
		setTestConfig(tempDir, fmt.Sprintf("s3://%s/test-prefix/", bucketName), bucketName, false, true, true, false)
		err := uploadToS3(ctx)
		assert.NoError(t, err)

		expectedKeys := []string{
			"test-prefix/file1.txt",
			"test-prefix/file2.txt",
			"test-prefix/subdir/file3.txt",
		}

		for _, key := range expectedKeys {
			_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
			})
			assert.NoError(t, err, "File %s should exist in S3", key)
		}
	})
}

func TestDownloadFromS3Directory(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-download-dir-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	testObjects := []string{
		"prefix/file1.txt",
		"prefix/file2.txt",
		"prefix/subdir/file3.txt",
	}

	for _, key := range testObjects {
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("test content")),
		})
		require.NoError(t, err)
	}

	t.Run("download directory", func(t *testing.T) {
		destDir := t.TempDir()
		setTestConfig(fmt.Sprintf("s3://%s/prefix/", bucketName), destDir, bucketName, false, false, true, false)
		err := downloadFromS3(ctx)
		assert.NoError(t, err)

		expectedFiles := []string{
			filepath.Join(destDir, "file1.txt"),
			filepath.Join(destDir, "file2.txt"),
			filepath.Join(destDir, "subdir", "file3.txt"),
		}

		for _, file := range expectedFiles {
			assert.FileExists(t, file)
		}
	})
}

func TestUploadToS3MultipleFiles(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-multiple-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	tempDir := t.TempDir()
	files := []string{
		filepath.Join(tempDir, "test1.txt"),
		filepath.Join(tempDir, "test2.txt"),
	}

	for _, file := range files {
		err := os.WriteFile(file, []byte("test content"), 0644)
		require.NoError(t, err)
	}

	t.Run("upload multiple files", func(t *testing.T) {
		pattern := filepath.Join(tempDir, "test*.txt")
		matches, err := filepath.Glob(pattern)
		require.NoError(t, err)
		require.Len(t, matches, 2)

		for i, file := range matches {
			key := fmt.Sprintf("file%d.txt", i+1)
			setTestConfig(file, fmt.Sprintf("s3://%s/%s", bucketName, key), bucketName, false, false, true, false)
			err := uploadToS3(ctx)
			assert.NoError(t, err)
		}

		expectedKeys := []string{
			"file1.txt",
			"file2.txt",
		}

		for _, key := range expectedKeys {
			_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
			})
			assert.NoError(t, err, "File %s should exist in S3", key)
		}
	})
}
func TestCalculateFileMD5(t *testing.T) {
	t.Run("CalculateMD5ForFile", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test-md5-*.txt")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		testContent := "Hello, World!\nThis is a test file.\n"
		_, err = tmpFile.WriteString(testContent)
		require.NoError(t, err)
		tmpFile.Close()

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

func TestCheckS3ObjectExists(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-check-exists-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	t.Run("object exists", func(t *testing.T) {
		key := "test-exists.txt"
		content := []byte("test content for existence check")

		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader(content),
		})
		require.NoError(t, err)

		exists, etag, metadata, err := checkS3ObjectExists(ctx, s3Client, bucketName, key)
		assert.NoError(t, err)
		assert.True(t, exists)
		assert.NotEmpty(t, etag)
		_ = metadata // metadata may be nil or empty for simple uploads
	})

	t.Run("object does not exist", func(t *testing.T) {
		key := "non-existent-key.txt"

		exists, etag, metadata, err := checkS3ObjectExists(ctx, s3Client, bucketName, key)
		_ = metadata // metadata may be nil for non-existent objects
		if err != nil {
			t.Logf("Error returned: %v", err)
			assert.Contains(t, err.Error(), "404")
			assert.False(t, exists)
			assert.Empty(t, etag)
		} else {
			assert.False(t, exists)
			assert.Empty(t, etag)
		}
	})

	t.Run("bucket does not exist", func(t *testing.T) {
		key := "test-key.txt"
		nonExistentBucket := "non-existent-bucket"

		exists, etag, metadata, err := checkS3ObjectExists(ctx, s3Client, nonExistentBucket, key)
		if err != nil {
			assert.False(t, exists)
			assert.Empty(t, etag)
			assert.Nil(t, metadata)
		} else {
			assert.False(t, exists)
			assert.Empty(t, etag)
			assert.Nil(t, metadata)
		}
	})
}

func TestCheckExistingUploadSkip(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-check-existing-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	restore := preserveGlobalVars()
	defer restore()

	tempDir := t.TempDir()
	sourceFile := filepath.Join(tempDir, "test-file.txt")
	testContent := []byte("This is test content for check-existing functionality")
	err := os.WriteFile(sourceFile, testContent, 0644)
	require.NoError(t, err)

	_, err = calculateFileMD5(sourceFile)
	require.NoError(t, err)

	s3Key := "test-check-existing.txt"

	t.Run("upload when object does not exist", func(t *testing.T) {
		setTestConfig(sourceFile, fmt.Sprintf("s3://%s/%s", bucketName, s3Key), bucketName, false, false, false, true)
		checkExisting = true

		output := captureStdout(func() {
			err := uploadToS3(ctx)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Uploading")
		assert.NotContains(t, output, "Skipping")

		_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Key),
		})
		assert.NoError(t, err)
	})

	t.Run("skip upload when object exists with same checksum", func(t *testing.T) {
		setTestConfig(sourceFile, fmt.Sprintf("s3://%s/%s", bucketName, s3Key), bucketName, false, false, false, true)
		checkExisting = true

		output := captureStdout(func() {
			err := uploadToS3(ctx)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Skipping")
		assert.Contains(t, output, "already exists with same checksum")
	})

	t.Run("upload when object exists with different checksum", func(t *testing.T) {
		differentContent := []byte("Different content with different checksum")
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Key),
			Body:   bytes.NewReader(differentContent),
		})
		require.NoError(t, err)

		setTestConfig(sourceFile, fmt.Sprintf("s3://%s/%s", bucketName, s3Key), bucketName, false, false, false, true)
		checkExisting = true
		verbose = true

		output := captureStdout(func() {
			err := uploadToS3(ctx)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Uploading")
		assert.Contains(t, output, "Object exists but no local MD5 in metadata, will upload")
	})

	t.Run("check-existing disabled", func(t *testing.T) {
		setTestConfig(sourceFile, fmt.Sprintf("s3://%s/%s-disabled", bucketName, s3Key), bucketName, false, false, false, true)
		checkExisting = false

		output := captureStdout(func() {
			err := uploadToS3(ctx)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Uploading")
		assert.NotContains(t, output, "Skipping")
		assert.NotContains(t, output, "already exists")
	})

	t.Run("check-existing with encryption disabled", func(t *testing.T) {
		setTestConfig(sourceFile, fmt.Sprintf("s3://%s/%s-encrypted", bucketName, s3Key), bucketName, true, false, false, true)
		checkExisting = true
		password = "testpassword"

		output := captureStdout(func() {
			err := uploadToS3(ctx)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Uploading")
		assert.NotContains(t, output, "Skipping")
	})

	t.Run("dry run with check-existing", func(t *testing.T) {
		setTestConfig(sourceFile, fmt.Sprintf("s3://%s/%s-dryrun", bucketName, s3Key), bucketName, false, false, false, true)
		checkExisting = true
		dryRun = true

		output := captureStdout(func() {
			err := uploadToS3(ctx)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Uploading")

		_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Key + "-dryrun"),
		})
		assert.Error(t, err)
	})
}

func TestCheckExistingDownloadSkip(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-check-existing-download-bucket"

	s3Client, cleanup := setupMinIOTest(t, ctx, bucketName)
	defer cleanup()

	restore := preserveGlobalVars()
	defer restore()

	tempDir := t.TempDir()
	testContent := []byte("This is test content for check-existing download functionality")
	s3Key := "test-check-existing-download.txt"

	_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
		Body:   bytes.NewReader(testContent),
	})
	require.NoError(t, err)

	localFile := filepath.Join(tempDir, "downloaded-file.txt")

	t.Run("download when local file does not exist", func(t *testing.T) {
		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, s3Key), localFile, bucketName, false, false, false, true)
		checkExisting = true

		output := captureStdout(func() {
			err := downloadFromS3(ctx)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Downloading")
		assert.NotContains(t, output, "Skipping")

		downloadedContent, err := os.ReadFile(localFile)
		assert.NoError(t, err)
		assert.Equal(t, testContent, downloadedContent)
	})

	t.Run("skip download when local file exists with same checksum", func(t *testing.T) {
		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, s3Key), localFile, bucketName, false, false, false, true)
		checkExisting = true

		output := captureStdout(func() {
			err := downloadFromS3(ctx)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Skipping")
		assert.Contains(t, output, "local file already exists with same checksum")
	})

	t.Run("download when local file exists with different checksum", func(t *testing.T) {
		differentContent := []byte("Different local content with different checksum")
		err := os.WriteFile(localFile, differentContent, 0644)
		require.NoError(t, err)

		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, s3Key), localFile, bucketName, false, false, false, true)
		checkExisting = true
		verbose = true

		output := captureStdout(func() {
			err := downloadFromS3(ctx)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Downloading")
		assert.Contains(t, output, "Local file exists but no remote MD5 in metadata, will download")

		downloadedContent, err := os.ReadFile(localFile)
		assert.NoError(t, err)
		assert.Equal(t, testContent, downloadedContent)
	})

	t.Run("check-existing disabled", func(t *testing.T) {
		localFileDisabled := filepath.Join(tempDir, "downloaded-file-disabled.txt")
		err := os.WriteFile(localFileDisabled, []byte("existing content"), 0644)
		require.NoError(t, err)

		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, s3Key), localFileDisabled, bucketName, false, false, false, true)
		checkExisting = false

		output := captureStdout(func() {
			err := downloadFromS3(ctx)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Downloading")
		assert.NotContains(t, output, "Skipping")
		assert.NotContains(t, output, "already exists")

		downloadedContent, err := os.ReadFile(localFileDisabled)
		assert.NoError(t, err)
		assert.Equal(t, testContent, downloadedContent)
	})

	t.Run("check-existing with encryption disabled", func(t *testing.T) {
		localFileEncrypted := filepath.Join(tempDir, "downloaded-file-encrypted.txt")
		err := os.WriteFile(localFileEncrypted, testContent, 0644)
		require.NoError(t, err)

		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, s3Key), localFileEncrypted, bucketName, true, false, false, true)
		checkExisting = true
		password = "testpassword"

		output := captureStdout(func() {
			err := downloadFromS3(ctx)
			assert.Error(t, err) // Expected to fail due to decryption mismatch
		})

		assert.Contains(t, output, "Downloading")
		assert.NotContains(t, output, "Skipping") // checkExisting should be bypassed with encryption
	})

	t.Run("dry run with check-existing", func(t *testing.T) {
		localFileDryRun := filepath.Join(tempDir, "downloaded-file-dryrun.txt")

		setTestConfig(fmt.Sprintf("s3://%s/%s", bucketName, s3Key), localFileDryRun, bucketName, false, false, false, true)
		checkExisting = true
		dryRun = true

		output := captureStdout(func() {
			err := downloadFromS3(ctx)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Downloading")

		_, err := os.Stat(localFileDryRun)
		assert.True(t, os.IsNotExist(err))
	})
}
