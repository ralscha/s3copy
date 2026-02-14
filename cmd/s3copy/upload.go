package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	manager "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
)

func uploadToS3(ctx context.Context) error {
	s3Client, err := getS3Client(ctx)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %w", err)
	}

	uploader := manager.New(s3Client)

	matches, err := filepath.Glob(source)
	if err != nil {
		return fmt.Errorf("invalid glob pattern: %w", err)
	}

	if len(matches) == 0 {
		info, err := os.Stat(source)
		if err != nil {
			return fmt.Errorf("failed to stat source: %w", err)
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
			return uploadDirectory(ctx, uploader, source, s3Key)
		}

		return uploadFile(ctx, uploader, source, s3Key)
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
				if err := uploadDirectory(ctx, uploader, match, dirS3Key); err != nil {
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
			if err := uploadFile(ctx, uploader, match, key); err != nil {
				return err
			}
		}
	}

	return nil
}

func uploadDirectory(ctx context.Context, uploader *manager.Client, localDir, s3Prefix string) error {
	type uploadTask struct {
		localPath string
		s3Key     string
	}

	return runWorkerPoolStream(ctx, maxWorkers, func(workerCtx context.Context, task uploadTask) error {
		if err := uploadFile(workerCtx, uploader, task.localPath, task.s3Key); err != nil {
			return fmt.Errorf("failed to upload %s: %w", task.localPath, err)
		}
		return nil
	}, func(producerCtx context.Context, taskChan chan<- uploadTask) error {
		walkErr := filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if producerCtx.Err() != nil {
				return producerCtx.Err()
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

			relPath, relErr := filepath.Rel(localDir, path)
			if relErr != nil {
				return relErr
			}

			task := uploadTask{
				localPath: path,
				s3Key:     strings.ReplaceAll(filepath.Join(s3Prefix, relPath), "\\", "/"),
			}

			select {
			case <-producerCtx.Done():
				return producerCtx.Err()
			case taskChan <- task:
				return nil
			}
		})

		if errors.Is(walkErr, context.Canceled) {
			return producerCtx.Err()
		}
		return walkErr
	})
}

func uploadFile(ctx context.Context, uploader *manager.Client, filePath, s3Key string) error {
	return uploadFileWithParams(ctx, uploader, bucket, s3Key, filePath, true)
}

func uploadFileWithParams(ctx context.Context, uploader *manager.Client, bucketName, s3Key, filePath string, checkSkipExisting bool) error {
	if checkSkipExisting {
		logInfo("Uploading %s to s3://%s/%s\n", filePath, bucketName, s3Key)
	}

	if dryRun {
		return nil
	}

	var localMD5 string
	localMTime := ""
	if !encrypt {
		if md5Hash, err := calculateFileMD5(filePath); err == nil {
			localMD5 = md5Hash
		} else {
			logVerbose("Warning: Could not calculate MD5 for %s: %v\n", filePath, err)
		}
	}

	if fileInfo, statErr := os.Stat(filePath); statErr == nil {
		localMTime = strconv.FormatInt(fileInfo.ModTime().Unix(), 10)
	} else {
		logVerbose("Warning: Could not stat %s for mtime metadata: %v\n", filePath, statErr)
	}

	if checkSkipExisting && !forceOverwrite && !encrypt && localMD5 != "" {
		s3Client, err := getS3Client(ctx)
		if err != nil {
			logVerbose("Warning: Could not get S3 client for checksum check: %v\n", err)
		} else {
			skip, err := compareFileChecksums(ctx, s3Client, bucketName, s3Key, localMD5)
			if err != nil {
				logVerbose("Warning: %v\n", err)
			} else if skip {
				logInfo("Skipping %s (file already exists on S3 with same checksum)\n", filePath)
				return nil
			}
		}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
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

		putInput := &manager.UploadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Key),
			Body:   reader,
		}
		if localMTime != "" {
			putInput.Metadata = map[string]string{
				"local-mtime": localMTime,
			}
		}

		_, uploadErr := uploader.UploadObject(ctx, putInput)

		if uploadErr != nil {
			_ = pipeReader.CloseWithError(uploadErr)
			<-errChan
			return uploadErr
		}

		closeWithLog(pipeReader, "pipe reader")
		if encErr := <-errChan; encErr != nil {
			return fmt.Errorf("encryption failed: %w", encErr)
		}
	} else {
		uploadInput := &manager.UploadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Key),
			Body:   reader,
		}
		if localMD5 != "" || localMTime != "" {
			uploadInput.Metadata = map[string]string{}
			if localMD5 != "" {
				uploadInput.Metadata["local-md5"] = localMD5
			}
			if localMTime != "" {
				uploadInput.Metadata["local-mtime"] = localMTime
			}
		}

		_, err = uploader.UploadObject(ctx, uploadInput)
		if err != nil {
			return err
		}
	}

	return nil
}
