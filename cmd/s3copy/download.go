package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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

	if skipExisting && !encrypt {
		if _, err := os.Stat(localPath); err == nil {
			localMD5, err := calculateFileMD5(localPath)
			if err != nil {
				logVerbose("Warning: Could not calculate MD5 for local file %s: %v\n", localPath, err)
			} else {
				s3Client, err := getS3Client(context.Background())
				if err != nil {
					logVerbose("Warning: Could not get S3 client for checksum check: %v\n", err)
				} else {
					skip, err := compareFileChecksums(context.Background(), s3Client, bucket, s3Key, localMD5)
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
			return fmt.Errorf("failed to create temp file: %v", err)
		}
		tempPath := tempFile.Name()
		defer func() {
			if err := os.Remove(tempPath); err != nil {
				fmt.Printf("Warning: failed to remove temp file %s: %v\n", tempPath, err)
			}
		}()

		if err := performS3Download(context.Background(), downloader, bucket, s3Key, tempFile); err != nil {
			closeWithLog(tempFile, tempPath)
			return err
		}
		closeWithLog(tempFile, tempPath)

		tempFileRead, err := os.Open(tempPath)
		if err != nil {
			return fmt.Errorf("failed to open temp file for decryption: %v", err)
		}
		defer closeWithLog(tempFileRead, tempPath)

		outFile, err := os.Create(localPath)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %v", localPath, err)
		}
		defer closeWithLog(outFile, localPath)

		if err := decryptStreamFromReader(outFile, tempFileRead); err != nil {
			return fmt.Errorf("decryption failed: %v", err)
		}
	} else {
		file, err := os.Create(localPath)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %v", localPath, err)
		}
		defer closeWithLog(file, localPath)

		if err := performS3Download(context.Background(), downloader, bucket, s3Key, file); err != nil {
			return err
		}
	}

	return nil
}
