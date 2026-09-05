package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	manager "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func downloadFromS3(ctx context.Context) error {
	s3Client, err := getS3Client(ctx)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %w", err)
	}

	downloader := manager.New(s3Client)

	parsedBucket, s3Key, err := parseS3Source(source, bucket)
	if err != nil {
		return err
	}
	bucket = parsedBucket

	objectExists := false
	if s3Key != "" && !strings.HasSuffix(s3Key, "/") {
		objectExists, _, _, err = checkS3ObjectExists(ctx, s3Client, bucket, s3Key)
		if err != nil {
			return fmt.Errorf("failed to inspect source object: %w", err)
		}
	}

	if objectExists {
		finalDestination := destination

		if strings.HasSuffix(destination, "/") || strings.HasSuffix(destination, string(filepath.Separator)) || destination == "." || destination == "./" {
			finalDestination, err = safeLocalPath(destination, pathBase(s3Key))
			if err != nil {
				return err
			}
		} else {
			if info, err := os.Stat(destination); err == nil && info.IsDir() {
				finalDestination, err = safeLocalPath(destination, pathBase(s3Key))
				if err != nil {
					return err
				}
			}
		}

		return downloadFile(ctx, downloader, s3Key, finalDestination)
	}

	prefix := s3Key
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	type downloadTask struct {
		s3Key     string
		localPath string
		isDir     bool
	}

	return runWorkerPoolStream(ctx, maxWorkers, func(workerCtx context.Context, task downloadTask) error {
		if task.isDir {
			if !dryRun {
				if err := os.MkdirAll(task.localPath, 0755); err != nil {
					return fmt.Errorf("failed to create directory %s: %w", task.localPath, err)
				}
			}
			return nil
		}

		if err := downloadFile(workerCtx, downloader, task.s3Key, task.localPath); err != nil {
			return fmt.Errorf("failed to download %s: %w", task.s3Key, err)
		}
		return nil
	}, func(producerCtx context.Context, taskChan chan<- downloadTask) error {
		foundObjects := false

		for paginator.HasMorePages() {
			result, pageErr := paginator.NextPage(producerCtx)
			if pageErr != nil {
				return fmt.Errorf("failed to list objects: %w", pageErr)
			}

			for _, obj := range result.Contents {
				if obj.Key == nil {
					continue
				}

				relPath := strings.TrimPrefix(*obj.Key, prefix)
				if relPath == "" {
					if strings.HasSuffix(*obj.Key, "/") {
						foundObjects = true
						continue
					}
					relPath = pathBase(*obj.Key)
				}
				foundObjects = true

				if shouldIgnoreFile(relPath) {
					logInfo("Ignoring: %s\n", *obj.Key)
					continue
				}

				isDir := strings.HasSuffix(*obj.Key, "/")
				if isDir {
					relPath = strings.TrimSuffix(relPath, "/")
				}
				localPath, pathErr := safeLocalPath(destination, relPath)
				if pathErr != nil {
					return fmt.Errorf("refusing unsafe S3 key %q: %w", *obj.Key, pathErr)
				}
				task := downloadTask{
					s3Key:     *obj.Key,
					localPath: localPath,
					isDir:     isDir,
				}

				select {
				case <-producerCtx.Done():
					return producerCtx.Err()
				case taskChan <- task:
				}
			}
		}

		if !foundObjects {
			return fmt.Errorf("no objects found with prefix: %s", prefix)
		}

		return nil
	})
}

func downloadFile(ctx context.Context, downloader *manager.Client, s3Key, localPath string) error {
	return downloadFileWithParams(ctx, downloader, bucket, s3Key, localPath, true)
}

func downloadFileWithParams(ctx context.Context, downloader *manager.Client, bucketName, s3Key, localPath string, checkSkipExisting bool) error {
	if checkSkipExisting {
		logInfo("Downloading s3://%s/%s to %s\n", bucketName, s3Key, localPath)
	}

	if dryRun {
		return nil
	}

	if checkSkipExisting && !forceOverwrite && !encrypt {
		if _, err := os.Stat(localPath); err == nil {
			localMD5, err := calculateFileMD5(localPath)
			if err != nil {
				logVerbose("Warning: Could not calculate MD5 for local file %s: %v\n", localPath, err)
			} else {
				s3Client, err := getS3Client(ctx)
				if err != nil {
					logVerbose("Warning: Could not get S3 client for checksum check: %v\n", err)
				} else {
					skip, err := compareFileChecksums(ctx, s3Client, bucketName, s3Key, localMD5)
					if err != nil {
						logVerbose("Warning: %v\n", err)
					} else if skip {
						logInfo("Skipping %s (local file already exists with same checksum)\n", localPath)
						return nil
					}
				}
			}
		}
	}

	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	if encrypt {
		tempFile, err := os.CreateTemp(filepath.Dir(localPath), ".s3copy-tmp-*")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %w", err)
		}
		tempPath := tempFile.Name()
		defer func() {
			if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
				logVerbose("Warning: failed to remove temp file %s: %v\n", tempPath, err)
			}
		}()

		_, err = downloader.DownloadObject(ctx, &manager.DownloadObjectInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(s3Key),
			WriterAt: tempFile,
		})

		closeWithLog(tempFile, tempPath)

		if err != nil {
			return err
		}

		tempFileRead, err := os.Open(tempPath)
		if err != nil {
			return fmt.Errorf("failed to open temp file for decryption: %w", err)
		}
		defer closeWithLog(tempFileRead, tempPath)

		decryptedTempFile, err := os.CreateTemp(filepath.Dir(localPath), ".s3copy-dec-*")
		if err != nil {
			return fmt.Errorf("failed to create temp decrypted file for %s: %w", localPath, err)
		}
		decryptedTempPath := decryptedTempFile.Name()
		defer func() {
			if err := os.Remove(decryptedTempPath); err != nil && !os.IsNotExist(err) {
				logVerbose("Warning: failed to remove temp file %s: %v\n", decryptedTempPath, err)
			}
		}()

		if err := decryptStreamFromReader(decryptedTempFile, tempFileRead); err != nil {
			closeWithLog(decryptedTempFile, decryptedTempPath)
			return fmt.Errorf("decryption failed: %w", err)
		}

		closeWithLog(decryptedTempFile, decryptedTempPath)

		if err := os.Rename(decryptedTempPath, localPath); err != nil {
			if removeErr := os.Remove(localPath); removeErr != nil && !os.IsNotExist(removeErr) {
				return fmt.Errorf("failed to replace existing file %s: %w", localPath, removeErr)
			}
			if renameErr := os.Rename(decryptedTempPath, localPath); renameErr != nil {
				return fmt.Errorf("failed to move decrypted file into place: %w", renameErr)
			}
		}
	} else {
		tempFile, err := os.CreateTemp(filepath.Dir(localPath), ".s3copy-dl-*")
		if err != nil {
			return fmt.Errorf("failed to create temp file for %s: %w", localPath, err)
		}
		tempPath := tempFile.Name()
		defer func() {
			if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
				logVerbose("Warning: failed to remove temp file %s: %v\n", tempPath, err)
			}
		}()

		_, err = downloader.DownloadObject(ctx, &manager.DownloadObjectInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(s3Key),
			WriterAt: tempFile,
		})
		closeWithLog(tempFile, tempPath)
		if err != nil {
			return err
		}

		if err := os.Rename(tempPath, localPath); err != nil {
			if removeErr := os.Remove(localPath); removeErr != nil && !os.IsNotExist(removeErr) {
				return fmt.Errorf("failed to replace existing file %s: %w", localPath, removeErr)
			}
			if renameErr := os.Rename(tempPath, localPath); renameErr != nil {
				return fmt.Errorf("failed to move downloaded file into place: %w", renameErr)
			}
		}
	}

	return nil
}
