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

		return downloadFile(ctx, downloader, s3Key, finalDestination)
	}

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(s3Key),
	})

	type downloadTask struct {
		s3Key     string
		localPath string
	}

	if err := os.MkdirAll(destination, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	return runWorkerPoolStream(ctx, maxWorkers, func(workerCtx context.Context, task downloadTask) error {
		if err := os.MkdirAll(filepath.Dir(task.localPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
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
				foundObjects = true

				relPath := strings.TrimPrefix(*obj.Key, s3Key)
				relPath = strings.TrimPrefix(relPath, "/")
				if relPath == "" {
					relPath = filepath.Base(*obj.Key)
				}

				task := downloadTask{
					s3Key:     *obj.Key,
					localPath: filepath.Join(destination, relPath),
				}

				select {
				case <-producerCtx.Done():
					return producerCtx.Err()
				case taskChan <- task:
				}
			}
		}

		if !foundObjects {
			return fmt.Errorf("no objects found with prefix: %s", s3Key)
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

	if encrypt {
		tempFile, err := os.CreateTemp(filepath.Dir(localPath), ".s3copy-tmp-*")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %w", err)
		}
		tempPath := tempFile.Name()
		defer func() {
			if err := os.Remove(tempPath); err != nil {
				fmt.Printf("Warning: failed to remove temp file %s: %v\n", tempPath, err)
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
				fmt.Printf("Warning: failed to remove temp file %s: %v\n", decryptedTempPath, err)
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
				fmt.Printf("Warning: failed to remove temp file %s: %v\n", tempPath, err)
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
