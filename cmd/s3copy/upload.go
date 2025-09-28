package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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
				exists, etag, metadata, err := checkS3ObjectExists(context.Background(), s3Client, bucket, s3Key)
				if err != nil {
					logVerbose("Warning: Could not check S3 object existence for %s: %v\n", s3Key, err)
				} else if exists {
					if etag == localMD5 {
						logInfo("Skipping %s (already exists with same checksum via ETag)\n", s3Key)
						return nil
					}
					if storedMD5, exists := metadata["local-md5"]; exists {
						if storedMD5 == localMD5 {
							logInfo("Skipping %s (already exists with same checksum via metadata)\n", s3Key)
							return nil
						} else {
							logVerbose("Object exists but checksum differs (local: %s, metadata: %s, etag: %s)\n", localMD5, storedMD5, etag)
						}
					} else {
						logVerbose("Object exists but no local MD5 in metadata, will upload (local: %s, etag: %s)\n", localMD5, etag)
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

	var localMD5 string
	if !encrypt {
		if md5Hash, err := calculateFileMD5(filePath); err == nil {
			localMD5 = md5Hash
		}
	}

	if encrypt {
		pipeReader, pipeWriter := io.Pipe()
		reader = pipeReader

		errChan := make(chan error, 1)
		go func() {
			defer pipeWriter.Close()
			errChan <- encryptStream(pipeWriter, file)
		}()

		uploadInput := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(s3Key),
			Body:   reader,
		}

		if err := retryOperation(func() error {
			_, err := uploader.Upload(context.Background(), uploadInput)
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
		uploadInput := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(s3Key),
			Body:   reader,
		}
		if localMD5 != "" {
			uploadInput.Metadata = map[string]string{
				"local-md5": localMD5,
			}
		}

		if err := retryOperation(func() error {
			_, err := uploader.Upload(context.Background(), uploadInput)
			return err
		}, "Upload", 3); err != nil {
			return err
		}
	}

	return nil
}
