package main

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func parseS3Source(s3Path, providedBucket string) (bucket, key string, err error) {
	if !strings.HasPrefix(s3Path, "s3://") {
		return "", "", fmt.Errorf("invalid S3 source format, expected s3://bucket/key")
	}

	trimmed := strings.TrimPrefix(s3Path, "s3://")
	if providedBucket != "" {
		bucket = providedBucket
		key = strings.TrimPrefix(trimmed, providedBucket+"/")
		if trimmed == providedBucket {
			key = ""
		}
		return bucket, key, nil
	}

	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		return "", "", fmt.Errorf("invalid S3 source format, expected s3://bucket/key")
	}

	bucket = parts[0]
	if len(parts) == 2 {
		key = parts[1]
	}
	return bucket, key, nil
}

func pathBase(s3Key string) string {
	return path.Base(strings.ReplaceAll(s3Key, "\\", "/"))
}

func parseS3Path(s3Path string, providedBucket string, isDir bool, localPath string) (bucket string, key string, err error) {
	s3Path = strings.TrimPrefix(s3Path, "s3://")

	if providedBucket == "" {
		parts := strings.SplitN(s3Path, "/", 2)
		if parts[0] == "" {
			return "", "", fmt.Errorf("invalid S3 format: bucket name is empty")
		}
		if len(parts) == 1 {
			bucket = parts[0]
			if !isDir {
				key = filepath.Base(localPath)
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

// checkS3ObjectExists checks if an S3 object exists and returns its ETag (MD5 for simple uploads) and metadata
func checkS3ObjectExists(ctx context.Context, s3Client *s3.Client, bucket, key string) (exists bool, etag string, metadata map[string]string, err error) {
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := s3Client.HeadObject(ctx, headInput)
	if err != nil {
		if _, ok := errors.AsType[*types.NoSuchKey](err); ok {
			return false, "", nil, nil
		}
		var apiErr interface{ ErrorCode() string }
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound") {
			return false, "", nil, nil
		}
		var statusErr interface{ HTTPStatusCode() int }
		if errors.As(err, &statusErr) && statusErr.HTTPStatusCode() == 404 {
			return false, "", nil, nil
		}
		return false, "", nil, err
	}

	etag = ""
	if result.ETag != nil {
		etag = strings.Trim(*result.ETag, "\"")
	}

	return true, etag, result.Metadata, nil
}

func listS3Objects(ctx context.Context) error {
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

	logInfo("Listing objects in bucket '%s'", bucket)
	if filter != "" {
		logInfo(" with prefix '%s'", filter)
	}
	logInfo(":\n\n")

	var totalObjects int64
	var totalSize int64

	if listDetailed {
		logInfo("%-50s %10s %-20s %-15s %-35s\n", "Key", "Size", "Last Modified", "Storage Class", "ETag")
		logInfo("%-50s %10s %-20s %-15s %-35s\n", strings.Repeat("-", 50), strings.Repeat("-", 10), strings.Repeat("-", 20), strings.Repeat("-", 15), strings.Repeat("-", 35))
	} else {
		logInfo("%-50s %10s %-20s\n", "Key", "Size", "Last Modified")
		logInfo("%-50s %10s %-20s\n", strings.Repeat("-", 50), strings.Repeat("-", 10), strings.Repeat("-", 20))
	}

	paginator := s3.NewListObjectsV2Paginator(s3Client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next page: %v", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			totalObjects++
			size := aws.ToInt64(obj.Size)
			totalSize += size
			lastModified := ""
			if obj.LastModified != nil {
				lastModified = obj.LastModified.Format("2006-01-02 15:04:05")
			}

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
				logInfo("%-50s %10s %-20s %-15s %-35s\n",
					truncateString(*obj.Key, 50),
					formatBytes(size),
					lastModified,
					storageClass,
					etag)
			} else {
				logInfo("%-50s %10s %-20s\n",
					truncateString(*obj.Key, 50),
					formatBytes(size),
					lastModified)
			}
		}
	}

	logInfo("\nTotal: %d objects, %s\n", totalObjects, formatBytes(totalSize))

	return nil
}
