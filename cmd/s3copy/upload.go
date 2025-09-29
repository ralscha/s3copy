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

	matches, err := filepath.Glob(source)
	if err != nil {
		return fmt.Errorf("invalid glob pattern: %v", err)
	}

	if len(matches) == 0 {
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

		return uploadFile(uploader, source, s3Key)
	}

	var parsedBucket, s3Key string

	if len(matches) == 1 {
		info, statErr := os.Stat(matches[0])
		isDir := statErr == nil && info.IsDir()

		if isDir && !recursive {
			return fmt.Errorf("source is a directory, use -r flag for recursive copy")
		}

		parsedBucket, s3Key, err = parseS3Path(destination, bucket, isDir, matches[0])
		if err != nil {
			return err
		}
	} else {
		parsedBucket, s3Key, err = parseS3Path(destination, bucket, true, "")
		if err != nil {
			return err
		}
	}

	if parsedBucket != "" {
		bucket = parsedBucket
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
				var dirS3Key string
				if len(matches) == 1 {
					dirS3Key = s3Key
				} else {
					dirS3Key = filepath.Join(s3Key, filepath.Base(match))
					dirS3Key = strings.ReplaceAll(dirS3Key, "\\", "/")
				}
				if err := uploadDirectory(uploader, match, dirS3Key); err != nil {
					return err
				}
			} else {
				logInfo("Skipping directory: %s (use -r flag for recursive copy)\n", match)
			}
		} else {
			key := s3Key
			if len(matches) > 1 {
				key = filepath.Join(s3Key, filepath.Base(match))
				key = strings.ReplaceAll(key, "\\", "/")
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
	return uploadFileWithParams(context.Background(), uploader, bucket, s3Key, filePath, true)
}

func uploadFileWithParams(ctx context.Context, uploader *manager.Uploader, bucketName, s3Key, filePath string, checkSkipExisting bool) error {
	if checkSkipExisting {
		logInfo("Uploading %s to s3://%s/%s\n", filePath, bucketName, s3Key)
	}

	if dryRun {
		return nil
	}

	var localMD5 string
	if !encrypt {
		if md5Hash, err := calculateFileMD5(filePath); err == nil {
			localMD5 = md5Hash
		} else {
			logVerbose("Warning: Could not calculate MD5 for %s: %v\n", filePath, err)
		}
	}

	if checkSkipExisting && skipExisting && !encrypt && localMD5 != "" {
		s3Client, err := getS3Client(ctx)
		if err != nil {
			logVerbose("Warning: Could not get S3 client for checksum check: %v\n", err)
		} else {
			skip, err := compareFileChecksums(ctx, s3Client, bucketName, s3Key, localMD5)
			if err != nil {
				logVerbose("Warning: %v\n", err)
			} else if skip {
				return nil
			}
		}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer closeWithLog(file, filePath)

	var reader io.Reader = file

	if encrypt {
		pipeReader, pipeWriter := io.Pipe()
		reader = pipeReader

		errChan := make(chan error, 1)
		go func() {
			defer closeWithLog(pipeWriter, "pipe writer")
			errChan <- encryptStream(pipeWriter, file)
		}()

		uploadErr := retryOperation(func() error {
			_, err := uploader.Upload(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(s3Key),
				Body:   reader,
			})
			return err
		}, "Upload", retries)

		if uploadErr != nil {
			return uploadErr
		}

		if encErr := <-errChan; encErr != nil {
			return fmt.Errorf("encryption failed: %v", encErr)
		}
	} else {
		uploadInput := &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Key),
			Body:   reader,
		}
		if localMD5 != "" {
			uploadInput.Metadata = map[string]string{
				"local-md5": localMD5,
			}
		}

		err = retryOperation(func() error {
			_, err := uploader.Upload(ctx, uploadInput)
			return err
		}, "Upload", retries)
		if err != nil {
			return err
		}
	}

	return nil
}
