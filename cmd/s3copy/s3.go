package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

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

// checkS3ObjectExists checks if an S3 object exists and returns its ETag (MD5 for simple uploads) and metadata
func checkS3ObjectExists(ctx context.Context, s3Client *s3.Client, bucket, key string) (exists bool, etag string, metadata map[string]string, err error) {
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := s3Client.HeadObject(ctx, headInput)
	if err != nil {
		var notFound *types.NoSuchKey
		if errors.As(err, &notFound) {
			return false, "", nil, nil
		}
		// Check for HTTP 404 status codes (which MinIO might return)
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "NotFound") {
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
