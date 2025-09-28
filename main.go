package main

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/joho/godotenv"
	ignore "github.com/sabhiram/go-gitignore"
	"github.com/urfave/cli/v3"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/term"
)

type Config struct {
	Endpoint     string
	AccessKey    string
	SecretKey    string
	Region       string
	UsePathStyle bool
}

type EncryptionParams struct {
	Salt  []byte
	Nonce []byte
}

// NonceManager handles secure nonce generation for chunked encryption
type NonceManager struct {
	baseNonce []byte
	counter   uint64
}

// NewNonceManager creates a new nonce manager with a random base nonce
func NewNonceManager() (*NonceManager, error) {
	baseNonce := make([]byte, chacha20poly1305.NonceSize)
	if _, err := rand.Read(baseNonce); err != nil {
		return nil, fmt.Errorf("failed to generate base nonce: %v", err)
	}

	return &NonceManager{
		baseNonce: baseNonce,
		counter:   0,
	}, nil
}

// NextNonce returns the next nonce in the sequence
func (nm *NonceManager) NextNonce() []byte {
	nonce := make([]byte, chacha20poly1305.NonceSize)
	copy(nonce, nm.baseNonce)

	binary.BigEndian.PutUint64(nonce[chacha20poly1305.NonceSize-8:], nm.counter)
	nm.counter++

	return nonce
}

// GetBaseNonce returns the base nonce for storage/transmission
func (nm *NonceManager) GetBaseNonce() []byte {
	return nm.baseNonce
}

// Reset resets the counter (used when starting decryption)
func (nm *NonceManager) Reset() {
	nm.counter = 0
}

var (
	config Config

	source         string
	destination    string
	bucket         string
	encrypt        bool
	password       string
	recursive      bool
	envFile        string
	listObjects    bool
	filter         string
	listDetailed   bool
	ignorePatterns string
	ignoreFile     string
	maxWorkers     = 5
	dryRun         bool
	quiet          bool
	verbose        bool
	timeout        int
	retries        int
	checkExisting  bool

	ignoreMatcher *ignore.GitIgnore

	s3ClientInstance *s3.Client
	s3ClientMutex    sync.Mutex
)

func logInfo(format string, args ...any) {
	if !quiet {
		fmt.Printf(format, args...)
	}
}

func logVerbose(format string, args ...any) {
	if verbose {
		fmt.Printf(format, args...)
	}
}

func main() {
	app := &cli.Command{
		Name:  "s3copy",
		Usage: "Copy files between local storage and S3-compatible storage with optional encryption",
		Description: `A CLI tool to copy files between local storage and S3-compatible storage.
Supports encryption using ChaCha20-Poly1305 with Argon2 key derivation.
Can copy single files or directories with glob pattern support.
Supports gitignore-style file filtering for selective copying.`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "source",
				Aliases:     []string{"s"},
				Usage:       "Source path (local file/directory or s3://bucket/key)",
				Destination: &source,
			},
			&cli.StringFlag{
				Name:        "destination",
				Aliases:     []string{"d"},
				Usage:       "Destination path (local file/directory or s3://bucket/key)",
				Destination: &destination,
			},
			&cli.StringFlag{
				Name:        "bucket",
				Aliases:     []string{"b"},
				Usage:       "S3 bucket name (required for S3 operations)",
				Destination: &bucket,
			},
			&cli.BoolFlag{
				Name:        "encrypt",
				Aliases:     []string{"e"},
				Usage:       "Enable encryption/decryption (required for both encrypting and decrypting files)",
				Destination: &encrypt,
			},
			&cli.StringFlag{
				Name:        "password",
				Aliases:     []string{"p"},
				Usage:       "Encryption password (omit value to prompt interactively)",
				Destination: &password,
			},
			&cli.BoolFlag{
				Name:        "recursive",
				Aliases:     []string{"r"},
				Usage:       "Copy directories recursively",
				Destination: &recursive,
			},
			&cli.StringFlag{
				Name:        "env",
				Usage:       "Path to .env file",
				Value:       ".env",
				Destination: &envFile,
			},
			&cli.BoolFlag{
				Name:        "list",
				Aliases:     []string{"l"},
				Usage:       "List objects in bucket",
				Destination: &listObjects,
			},
			&cli.StringFlag{
				Name:        "filter",
				Aliases:     []string{"f"},
				Usage:       "Filter objects by prefix (used with --list)",
				Destination: &filter,
			},
			&cli.BoolFlag{
				Name:        "detailed",
				Usage:       "Show detailed information when listing (storage class, ETag, etc.)",
				Destination: &listDetailed,
			},
			&cli.StringFlag{
				Name:        "ignore",
				Usage:       "Comma-separated list of patterns to ignore (gitignore syntax)",
				Destination: &ignorePatterns,
			},
			&cli.StringFlag{
				Name:        "ignore-file",
				Usage:       "Path to file containing ignore patterns (one per line, gitignore syntax)",
				Destination: &ignoreFile,
			},
			&cli.IntFlag{
				Name:        "max-workers",
				Usage:       "Maximum number of concurrent workers for uploads/downloads",
				Value:       5,
				Destination: &maxWorkers,
			},
			&cli.BoolFlag{
				Name:        "dry-run",
				Usage:       "Show what would be done without actually performing the operations",
				Destination: &dryRun,
			},
			&cli.BoolFlag{
				Name:        "quiet",
				Usage:       "Suppress non-error output",
				Destination: &quiet,
			},
			&cli.BoolFlag{
				Name:        "verbose",
				Usage:       "Enable verbose output",
				Destination: &verbose,
			},
			&cli.IntFlag{
				Name:        "timeout",
				Usage:       "Timeout for operations in seconds (0 for no timeout)",
				Value:       0,
				Destination: &timeout,
			},
			&cli.IntFlag{
				Name:        "retries",
				Usage:       "Number of retry attempts for failed operations",
				Value:       3,
				Destination: &retries,
			},
			&cli.BoolFlag{
				Name:        "check-existing",
				Usage:       "Check if file already exists with same checksum before uploading/downloading",
				Destination: &checkExisting,
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if password == "" && cmd.IsSet("password") {
				password = "PROMPT"
			}

			if !listObjects {
				if source == "" {
					return ctx, fmt.Errorf("source is required when not listing objects")
				}
				if destination == "" {
					return ctx, fmt.Errorf("destination is required when not listing objects")
				}
			} else {
				if bucket == "" {
					return ctx, fmt.Errorf("bucket is required when listing objects")
				}
			}
			return ctx, nil
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return runCopy()
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runCopy() error {
	if err := godotenv.Load(envFile); err != nil {
		if !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Warning: Could not load %s file: %v\n", envFile, err)
		}
	}

	config = Config{
		Endpoint:     getEnvOrDefault("S3COPY_ENDPOINT", ""),
		AccessKey:    getEnvOrDefault("S3COPY_ACCESS_KEY", ""),
		SecretKey:    getEnvOrDefault("S3COPY_SECRET_KEY", ""),
		Region:       getEnvOrDefault("S3COPY_REGION", "us-east-1"),
		UsePathStyle: getEnvOrDefault("S3COPY_USE_PATH_STYLE", "false") == "true",
	}

	if config.AccessKey == "" || config.SecretKey == "" {
		return fmt.Errorf("missing required environment variables (S3COPY_ACCESS_KEY, S3COPY_SECRET_KEY)")
	}

	if err := initializeIgnoreMatcher(); err != nil {
		return fmt.Errorf("error initializing ignore patterns: %v", err)
	}

	if listObjects {
		if err := listS3Objects(); err != nil {
			return fmt.Errorf("error listing objects: %v", err)
		}
		return nil
	}

	sourceIsS3 := strings.HasPrefix(source, "s3://")
	destIsS3 := strings.HasPrefix(destination, "s3://")

	if sourceIsS3 && destIsS3 {
		return fmt.Errorf("S3 to S3 copy is not supported")
	}

	if !sourceIsS3 && !destIsS3 {
		return fmt.Errorf("at least one of source or destination must be S3")
	}

	if encrypt {
		if password == "" || password == "PROMPT" {
			var err error
			password, err = getPasswordFromUser()
			if err != nil {
				return fmt.Errorf("error getting password: %v", err)
			}
			if password == "" {
				return fmt.Errorf("empty password provided for encryption")
			}
		}
	}

	if sourceIsS3 {
		ctx := context.Background()
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
			defer cancel()
		}
		if err := downloadFromS3(ctx); err != nil {
			return fmt.Errorf("error downloading from S3: %v", err)
		}
	} else {
		ctx := context.Background()
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
			defer cancel()
		}
		if err := uploadToS3(ctx); err != nil {
			return fmt.Errorf("error uploading to S3: %v", err)
		}
	}

	logInfo("Copy operation completed successfully!\n")
	return nil
}

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
		return nil, fmt.Errorf("failed to create S3 config: %v", err)
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

func parseS3Path(s3Path string, providedBucket string, isDir bool, localPath string) (bucket string, key string, err error) {
	s3Path = strings.TrimPrefix(s3Path, "s3://")

	if providedBucket == "" {
		parts := strings.SplitN(s3Path, "/", 2)
		if len(parts) == 1 {
			bucket = parts[0]
			if !isDir {
				key = filepath.Base(localPath)
			} else {
				return "", "", fmt.Errorf("invalid S3 format for directory, use s3://bucket/key or specify bucket with -b flag")
			}
		} else if len(parts) == 2 {
			bucket = parts[0]
			key = parts[1]
			if (key == "" || key == "/") && !isDir {
				key = filepath.Base(localPath)
			} else if strings.HasSuffix(key, "/") && !isDir {
				key = key + filepath.Base(localPath)
			}
		} else {
			return "", "", fmt.Errorf("invalid S3 format, use s3://bucket/key or specify bucket with -b flag")
		}
	} else {
		bucket = providedBucket
		key = strings.TrimPrefix(s3Path, providedBucket+"/")
		if key == "" && !isDir {
			key = filepath.Base(localPath)
		} else if strings.HasSuffix(key, "/") && !isDir {
			key = key + filepath.Base(localPath)
		}
	}

	return bucket, key, nil
}

func uploadToS3(ctx context.Context) error {
	s3Client, err := getS3Client(ctx)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %v", err)
	}

	uploader := manager.NewUploader(s3Client)

	info, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("failed to stat source: %v", err)
	}

	parsedBucket, s3Key, err := parseS3Path(destination, bucket, info.IsDir(), source)
	if err != nil {
		return err
	}

	if parsedBucket != "" {
		bucket = parsedBucket
	}

	if info.IsDir() {
		if !recursive {
			return fmt.Errorf("source is a directory, use -r flag for recursive copy")
		}
		return uploadDirectory(uploader, source, s3Key)
	}

	matches, err := filepath.Glob(source)
	if err != nil {
		return fmt.Errorf("invalid glob pattern: %v", err)
	}

	if len(matches) == 0 {
		return fmt.Errorf("no files match the pattern: %s", source)
	}

	for _, match := range matches {
		if shouldIgnoreFile(match) {
			logInfo("Ignoring: %s\n", match)
			continue
		}

		info, err := os.Stat(match)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Could not stat %s: %v\n", match, err)
			continue
		}

		if info.IsDir() {
			if recursive {
				if err := uploadDirectory(uploader, match, filepath.Join(s3Key, filepath.Base(match))); err != nil {
					return err
				}
			}
		} else {
			key := s3Key
			if len(matches) > 1 {
				key = filepath.Join(s3Key, filepath.Base(match))
			}
			if err := uploadFile(uploader, match, key); err != nil {
				return err
			}
		}
	}

	return nil
}

func uploadDirectory(uploader *manager.Uploader, localDir, s3Prefix string) error {
	type uploadTask struct {
		localPath string
		s3Key     string
	}
	var tasks []uploadTask

	err := filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			if shouldIgnoreFile(path) {
				logInfo("Ignoring directory: %s\n", path)
				return filepath.SkipDir
			}
			return nil
		}

		if shouldIgnoreFile(path) {
			logInfo("Ignoring file: %s\n", path)
			return nil
		}

		relPath, err := filepath.Rel(localDir, path)
		if err != nil {
			return err
		}

		s3Key := filepath.Join(s3Prefix, relPath)
		s3Key = strings.ReplaceAll(s3Key, "\\", "/")

		tasks = append(tasks, uploadTask{
			localPath: path,
			s3Key:     s3Key,
		})
		return nil
	})

	if err != nil {
		return err
	}

	return runWorkerPool(tasks, maxWorkers, func(task uploadTask) error {
		if err := uploadFile(uploader, task.localPath, task.s3Key); err != nil {
			return fmt.Errorf("failed to upload %s: %v", task.localPath, err)
		}
		return nil
	})
}

// calculateFileMD5 calculates the MD5 checksum of a file
func calculateFileMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// checkS3ObjectExists checks if an S3 object exists and returns its ETag (MD5 for simple uploads)
func checkS3ObjectExists(ctx context.Context, s3Client *s3.Client, bucket, key string) (exists bool, etag string, err error) {
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := s3Client.HeadObject(ctx, headInput)
	if err != nil {
		var notFound *types.NoSuchKey
		if errors.As(err, &notFound) {
			return false, "", nil
		}
		// Check for HTTP 404 status codes (which MinIO might return)
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "NotFound") {
			return false, "", nil
		}
		return false, "", err
	}

	etag = ""
	if result.ETag != nil {
		etag = strings.Trim(*result.ETag, "\"")
	}

	return true, etag, nil
}

// retryOperation executes an operation with retry logic
func retryOperation(operation func() error, operationType string, maxAttempts int) error {
	var lastErr error
	for attempts := range maxAttempts {
		lastErr = operation()
		if lastErr == nil {
			return nil
		}
		if attempts < maxAttempts-1 {
			logVerbose("%s attempt %d failed, retrying...\n", operationType, attempts+1)
		}
	}
	return fmt.Errorf("failed to %s after %d attempts: %v", strings.ToLower(operationType), maxAttempts, lastErr)
}

// runWorkerPool executes tasks using a worker pool pattern
func runWorkerPool[T any](tasks []T, maxWorkers int, worker func(T) error) error {
	taskChan := make(chan T, len(tasks))
	errChan := make(chan error, len(tasks))
	var wg sync.WaitGroup

	for i := 0; i < maxWorkers && i < len(tasks); i++ {
		wg.Go(func() {
			for task := range taskChan {
				if err := worker(task); err != nil {
					errChan <- err
					return
				}
			}
		})
	}

	for _, task := range tasks {
		taskChan <- task
	}
	close(taskChan)

	wg.Wait()
	close(errChan)

	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

func uploadFile(uploader *manager.Uploader, filePath, s3Key string) error {
	logInfo("Uploading %s to s3://%s/%s\n", filePath, bucket, s3Key)

	if dryRun {
		return nil
	}

	if checkExisting && !encrypt {
		localMD5, err := calculateFileMD5(filePath)
		if err != nil {
			logVerbose("Warning: Could not calculate MD5 for %s: %v\n", filePath, err)
		} else {
			s3Client, err := getS3Client(context.Background())
			if err != nil {
				logVerbose("Warning: Could not get S3 client for checksum check: %v\n", err)
			} else {
				exists, etag, err := checkS3ObjectExists(context.Background(), s3Client, bucket, s3Key)
				if err != nil {
					logVerbose("Warning: Could not check S3 object existence for %s: %v\n", s3Key, err)
				} else if exists {
					if etag == localMD5 {
						logInfo("Skipping %s (already exists with same checksum)\n", s3Key)
						return nil
					} else {
						logVerbose("Object exists but checksum differs (local: %s, remote: %s)\n", localMD5, etag)
					}
				}
			}
		}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	var reader io.Reader = file

	if encrypt {
		pipeReader, pipeWriter := io.Pipe()
		reader = pipeReader

		errChan := make(chan error, 1)
		go func() {
			defer pipeWriter.Close()
			errChan <- encryptStream(pipeWriter, file)
		}()

		if err := retryOperation(func() error {
			_, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(s3Key),
				Body:   reader,
			})
			return err
		}, "Upload", 3); err != nil {
			return err
		}

		select {
		case encErr := <-errChan:
			if encErr != nil {
				return fmt.Errorf("encryption failed: %v", encErr)
			}
		default:
			// Encryption still running or completed successfully
		}
	} else {
		if err := retryOperation(func() error {
			_, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(s3Key),
				Body:   reader,
			})
			return err
		}, "Upload", 3); err != nil {
			return err
		}
	}

	return nil
}

func downloadFromS3(ctx context.Context) error {
	s3Client, err := getS3Client(ctx)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %v", err)
	}

	downloader := manager.NewDownloader(s3Client)

	s3Path := strings.TrimPrefix(source, "s3://")
	var s3Key string

	if bucket == "" {
		parts := strings.SplitN(s3Path, "/", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid S3 source format, use s3://bucket/key or specify bucket with -b flag")
		}
		bucket = parts[0]
		s3Key = parts[1]
	} else {
		s3Key = strings.TrimPrefix(s3Path, bucket+"/")
	}

	_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s3Key),
	})

	if err == nil {
		finalDestination := destination

		if strings.HasSuffix(destination, "/") || destination == "." || destination == "./" {
			filename := filepath.Base(s3Key)
			finalDestination = filepath.Join(destination, filename)
		} else {
			if info, err := os.Stat(destination); err == nil && info.IsDir() {
				filename := filepath.Base(s3Key)
				finalDestination = filepath.Join(destination, filename)
			}
		}

		return downloadFile(downloader, s3Key, finalDestination)
	}

	result, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(s3Key),
	})

	if err != nil {
		return fmt.Errorf("failed to list objects: %v", err)
	}

	if len(result.Contents) == 0 {
		return fmt.Errorf("no objects found with prefix: %s", s3Key)
	}

	if err := os.MkdirAll(destination, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %v", err)
	}

	type downloadTask struct {
		s3Key     string
		localPath string
	}
	var tasks []downloadTask

	for _, obj := range result.Contents {
		relPath := strings.TrimPrefix(*obj.Key, s3Key)
		relPath = strings.TrimPrefix(relPath, "/")

		if relPath == "" {
			relPath = filepath.Base(*obj.Key)
		}

		localPath := filepath.Join(destination, relPath)

		if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %v", err)
		}

		tasks = append(tasks, downloadTask{
			s3Key:     *obj.Key,
			localPath: localPath,
		})
	}

	return runWorkerPool(tasks, maxWorkers, func(task downloadTask) error {
		if err := downloadFile(downloader, task.s3Key, task.localPath); err != nil {
			return fmt.Errorf("failed to download %s: %v", task.s3Key, err)
		}
		return nil
	})
}

func downloadFile(downloader *manager.Downloader, s3Key, localPath string) error {
	logInfo("Downloading s3://%s/%s to %s\n", bucket, s3Key, localPath)

	if dryRun {
		return nil
	}

	if checkExisting && !encrypt {
		if _, err := os.Stat(localPath); err == nil {
			localMD5, err := calculateFileMD5(localPath)
			if err != nil {
				logVerbose("Warning: Could not calculate MD5 for local file %s: %v\n", localPath, err)
			} else {
				s3Client, err := getS3Client(context.Background())
				if err != nil {
					logVerbose("Warning: Could not get S3 client for checksum check: %v\n", err)
				} else {
					exists, etag, err := checkS3ObjectExists(context.Background(), s3Client, bucket, s3Key)
					if err != nil {
						logVerbose("Warning: Could not check S3 object existence for %s: %v\n", s3Key, err)
					} else if exists {
						if etag == localMD5 {
							logInfo("Skipping %s (local file already exists with same checksum)\n", localPath)
							return nil
						} else {
							logVerbose("Local file exists but checksum differs (local: %s, remote: %s)\n", localMD5, etag)
						}
					}
				}
			}
		}
	}

	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %v", localPath, err)
	}
	defer file.Close()

	if encrypt {
		pipeReader, pipeWriter := io.Pipe()

		decryptErr := make(chan error, 1)
		go func() {
			defer pipeReader.Close()
			decryptErr <- decryptStreamFromReader(file, pipeReader)
		}()

		downloadErr := retryOperation(func() error {
			_, err := downloader.Download(context.Background(),
				&writeAtWrapper{w: pipeWriter},
				&s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(s3Key),
				})
			return err
		}, "Download", 3)

		pipeWriter.Close()

		if downloadErr != nil {
			return downloadErr
		}

		if err := <-decryptErr; err != nil {
			return fmt.Errorf("decryption failed: %v", err)
		}
	} else {
		return retryOperation(func() error {
			_, err := downloader.Download(context.Background(), file, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(s3Key),
			})
			return err
		}, "Download", 3)
	}

	return nil
}

// writeAtWrapper wraps an io.Writer to implement io.WriterAt for sequential writes
type writeAtWrapper struct {
	w      io.Writer
	offset int64
	mu     sync.Mutex
}

func (w *writeAtWrapper) WriteAt(p []byte, off int64) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if off != w.offset {
		return 0, fmt.Errorf("non-sequential write at offset %d, expected %d", off, w.offset)
	}

	n, err = w.w.Write(p)
	w.offset += int64(n)
	return n, err
}

func encryptStream(writer io.Writer, reader io.Reader) error {
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("failed to generate salt: %v", err)
	}

	nonceManager, err := NewNonceManager()
	if err != nil {
		return err
	}

	if _, err := writer.Write(salt); err != nil {
		return fmt.Errorf("failed to write salt: %v", err)
	}
	if _, err := writer.Write(nonceManager.GetBaseNonce()); err != nil {
		return fmt.Errorf("failed to write base nonce: %v", err)
	}

	key := argon2.IDKey([]byte(password), salt, 3, 64*1024, 4, 32)

	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return fmt.Errorf("failed to create AEAD cipher: %v", err)
	}

	buf := make([]byte, 1024*1024) // 1MB chunks
	chunkCount := uint64(0)

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			chunkNonce := nonceManager.NextNonce()
			encryptedChunk := aead.Seal(nil, chunkNonce, buf[:n], nil)
			chunkSizeBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(chunkSizeBytes, uint32(len(encryptedChunk)))

			if _, writeErr := writer.Write(chunkSizeBytes); writeErr != nil {
				return fmt.Errorf("failed to write chunk size: %v", writeErr)
			}
			if _, writeErr := writer.Write(encryptedChunk); writeErr != nil {
				return fmt.Errorf("failed to write encrypted chunk: %v", writeErr)
			}

			chunkCount++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read from source: %v", err)
		}
	}

	return nil
}

func decryptStreamFromReader(writer io.Writer, reader io.Reader) error {
	header := make([]byte, 44) // 32 (salt) + 12 (base nonce)
	if _, err := io.ReadFull(reader, header); err != nil {
		return fmt.Errorf("failed to read encryption header: %v", err)
	}

	salt := header[:32]
	baseNonce := header[32:44]

	key := argon2.IDKey([]byte(password), salt, 3, 64*1024, 4, 32)

	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return fmt.Errorf("failed to create AEAD cipher: %v", err)
	}

	nonceManager := &NonceManager{
		baseNonce: make([]byte, chacha20poly1305.NonceSize),
		counter:   0,
	}
	copy(nonceManager.baseNonce, baseNonce)

	for {
		chunkSizeBytes := make([]byte, 4)
		if _, err := io.ReadFull(reader, chunkSizeBytes); err != nil {
			if err == io.EOF {
				break // Normal end of stream
			}
			return fmt.Errorf("failed to read chunk size: %v", err)
		}

		chunkSize := binary.BigEndian.Uint32(chunkSizeBytes)

		encryptedChunk := make([]byte, chunkSize)
		if _, err := io.ReadFull(reader, encryptedChunk); err != nil {
			return fmt.Errorf("failed to read encrypted chunk: %v", err)
		}

		chunkNonce := nonceManager.NextNonce()
		plaintext, err := aead.Open(nil, chunkNonce, encryptedChunk, nil)
		if err != nil {
			return fmt.Errorf("decryption failed (wrong password or corrupted data?): %v", err)
		}

		if _, err := writer.Write(plaintext); err != nil {
			return fmt.Errorf("failed to write decrypted data: %v", err)
		}
	}

	return nil
}

func listS3Objects() error {
	ctx := context.Background()
	s3Client, err := getS3Client(ctx)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %v", err)
	}

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	if filter != "" {
		input.Prefix = aws.String(filter)
	}

	fmt.Printf("Listing objects in bucket '%s'", bucket)
	if filter != "" {
		fmt.Printf(" with prefix '%s'", filter)
	}
	fmt.Println(":")
	fmt.Println()

	var totalObjects int64
	var totalSize int64

	if listDetailed {
		fmt.Printf("%-50s %10s %-20s %-15s %-35s\n", "Key", "Size", "Last Modified", "Storage Class", "ETag")
		fmt.Printf("%-50s %10s %-20s %-15s %-35s\n", strings.Repeat("-", 50), strings.Repeat("-", 10), strings.Repeat("-", 20), strings.Repeat("-", 15), strings.Repeat("-", 35))
	} else {
		fmt.Printf("%-50s %10s %-20s\n", "Key", "Size", "Last Modified")
		fmt.Printf("%-50s %10s %-20s\n", strings.Repeat("-", 50), strings.Repeat("-", 10), strings.Repeat("-", 20))
	}

	paginator := s3.NewListObjectsV2Paginator(s3Client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next page: %v", err)
		}

		for _, obj := range page.Contents {
			totalObjects++
			totalSize += *obj.Size

			if listDetailed {
				storageClass := ""
				if obj.StorageClass != "" {
					storageClass = string(obj.StorageClass)
				}
				etag := ""
				if obj.ETag != nil {
					etag = strings.Trim(*obj.ETag, "\"")
					if len(etag) > 32 {
						etag = etag[:32] + "..."
					}
				}
				fmt.Printf("%-50s %10s %-20s %-15s %-35s\n",
					truncateString(*obj.Key, 50),
					formatBytes(*obj.Size),
					obj.LastModified.Format("2006-01-02 15:04:05"),
					storageClass,
					etag)
			} else {
				fmt.Printf("%-50s %10s %-20s\n",
					truncateString(*obj.Key, 50),
					formatBytes(*obj.Size),
					obj.LastModified.Format("2006-01-02 15:04:05"))
			}
		}
	}

	fmt.Println()
	fmt.Printf("Total: %d objects, %s\n", totalObjects, formatBytes(totalSize))

	return nil
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

func initializeIgnoreMatcher() error {
	var patterns []string

	if ignorePatterns != "" {
		for pattern := range strings.SplitSeq(ignorePatterns, ",") {
			trimmed := strings.TrimSpace(pattern)
			if trimmed != "" {
				patterns = append(patterns, trimmed)
			}
		}
	}

	if ignoreFile != "" {
		filePatterns, err := readIgnoreFile(ignoreFile)
		if err != nil {
			return fmt.Errorf("failed to read ignore file %s: %v", ignoreFile, err)
		}
		patterns = append(patterns, filePatterns...)
	}

	if len(patterns) > 0 {
		ignoreMatcher = ignore.CompileIgnoreLines(patterns...)
	}

	return nil
}

func readIgnoreFile(filePath string) ([]string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var patterns []string
	for line := range strings.SplitSeq(string(content), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			patterns = append(patterns, trimmed)
		}
	}

	return patterns, nil
}

func shouldIgnoreFile(filePath string) bool {
	if ignoreMatcher == nil {
		return false
	}

	var relativePath string
	if filepath.IsAbs(filePath) {
		if rel, err := filepath.Rel(source, filePath); err == nil && !strings.HasPrefix(rel, "..") {
			relativePath = rel
		} else {
			relativePath = filepath.Base(filePath)
		}
	} else {
		relativePath = filePath
	}

	normalizedPath := strings.ReplaceAll(relativePath, "\\", "/")
	return ignoreMatcher.MatchesPath(normalizedPath)
}
