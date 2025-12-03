package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type FileInfo struct {
	Path    string
	Size    int64
	MD5Hash string
	IsDir   bool
	RelPath string // Relative path from the base directory
}

type SyncResult struct {
	Uploaded   []string
	Downloaded []string
	Deleted    []string
	Errors     []string
}

func syncDirectories(ctx context.Context) error {
	sourceIsS3 := strings.HasPrefix(source, "s3://")

	s3Client, err := getS3Client(ctx)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %v", err)
	}

	var result SyncResult

	if sourceIsS3 {
		result, err = syncS3ToLocal(ctx, s3Client)
	} else {
		result, err = syncLocalToS3(ctx, s3Client)
	}

	if err != nil {
		return err
	}

	printSyncSummary(result)
	return nil
}

func syncS3ToLocal(ctx context.Context, s3Client *s3.Client) (SyncResult, error) {
	var result SyncResult

	s3Path := strings.TrimPrefix(source, "s3://")
	var s3Bucket, s3Prefix string

	if bucket == "" {
		parts := strings.SplitN(s3Path, "/", 2)
		if len(parts) < 1 {
			return result, fmt.Errorf("invalid S3 source format")
		}
		s3Bucket = parts[0]
		if len(parts) > 1 {
			s3Prefix = parts[1]
		}
	} else {
		s3Bucket = bucket
		s3Prefix = strings.TrimPrefix(s3Path, s3Bucket+"/")
	}

	if s3Prefix != "" && !strings.HasSuffix(s3Prefix, "/") {
		s3Prefix += "/"
	}

	s3Files, err := listS3Files(ctx, s3Client, s3Bucket, s3Prefix)
	if err != nil {
		return result, fmt.Errorf("failed to list S3 files: %v", err)
	}

	localFiles, err := listLocalFiles(destination)
	if err != nil {
		return result, fmt.Errorf("failed to list local files: %v", err)
	}

	s3FileMap := make(map[string]FileInfo)
	localFileMap := make(map[string]FileInfo)

	for _, file := range s3Files {
		s3FileMap[file.RelPath] = file
	}

	for _, file := range localFiles {
		localFileMap[file.RelPath] = file
	}

	var toDownload []FileInfo
	var toDelete []FileInfo

	for relPath, s3File := range s3FileMap {
		if localFile, exists := localFileMap[relPath]; exists {
			if !filesAreSameWithMetadataCheck(ctx, s3Client, localFile, s3File, s3Bucket) {
				toDownload = append(toDownload, s3File)
			}
		} else {
			toDownload = append(toDownload, s3File)
		}
	}

	for relPath, localFile := range localFileMap {
		if _, exists := s3FileMap[relPath]; !exists {
			toDelete = append(toDelete, localFile)
		}
	}

	if len(toDownload) > 0 {
		if err := downloadFiles(ctx, s3Client, s3Bucket, toDownload, &result); err != nil {
			return result, err
		}
	}

	if len(toDelete) > 0 {
		if err := deleteLocalFiles(toDelete, &result); err != nil {
			return result, err
		}
	}

	return result, nil
}

func syncLocalToS3(ctx context.Context, s3Client *s3.Client) (SyncResult, error) {
	var result SyncResult

	s3Path := strings.TrimPrefix(destination, "s3://")
	var s3Bucket, s3Prefix string

	if bucket == "" {
		parts := strings.SplitN(s3Path, "/", 2)
		if len(parts) < 1 {
			return result, fmt.Errorf("invalid S3 destination format")
		}
		s3Bucket = parts[0]
		if len(parts) > 1 {
			s3Prefix = parts[1]
		}
	} else {
		s3Bucket = bucket
		s3Prefix = strings.TrimPrefix(s3Path, s3Bucket+"/")
	}

	if s3Prefix != "" && !strings.HasSuffix(s3Prefix, "/") {
		s3Prefix += "/"
	}

	localFiles, err := listLocalFiles(source)
	if err != nil {
		return result, fmt.Errorf("failed to list local files: %v", err)
	}

	s3Files, err := listS3Files(ctx, s3Client, s3Bucket, s3Prefix)
	if err != nil {
		return result, fmt.Errorf("failed to list S3 files: %v", err)
	}

	localFileMap := make(map[string]FileInfo)
	s3FileMap := make(map[string]FileInfo)

	for _, file := range localFiles {
		localFileMap[file.RelPath] = file
	}

	for _, file := range s3Files {
		s3FileMap[file.RelPath] = file
	}

	var toUpload []FileInfo
	var toDelete []FileInfo

	for relPath, localFile := range localFileMap {
		if s3File, exists := s3FileMap[relPath]; exists {
			if !filesAreSameWithMetadataCheck(ctx, s3Client, localFile, s3File, s3Bucket) {
				toUpload = append(toUpload, localFile)
			}
		} else {
			toUpload = append(toUpload, localFile)
		}
	}

	for relPath, s3File := range s3FileMap {
		if _, exists := localFileMap[relPath]; !exists {
			toDelete = append(toDelete, s3File)
		}
	}

	if len(toUpload) > 0 {
		if err := uploadFiles(ctx, s3Client, s3Bucket, s3Prefix, toUpload, &result); err != nil {
			return result, err
		}
	}

	if len(toDelete) > 0 {
		if err := deleteS3Files(ctx, s3Client, s3Bucket, toDelete, &result); err != nil {
			return result, err
		}
	}

	return result, nil
}

func listS3Files(ctx context.Context, s3Client *s3.Client, bucket, prefix string) ([]FileInfo, error) {
	var files []FileInfo

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	paginator := s3.NewListObjectsV2Paginator(s3Client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			key := *obj.Key

			if shouldIgnoreFile(key) {
				continue
			}

			relPath := key
			if prefix != "" {
				relPath = strings.TrimPrefix(key, prefix)
			}

			if relPath == "" {
				continue
			}

			relPath = decodeS3Key(relPath)

			var size int64
			if obj.Size != nil {
				size = *obj.Size
			}

			file := FileInfo{
				Path:    key,
				RelPath: relPath,
				Size:    size,
				IsDir:   false,
			}

			if obj.ETag != nil {
				file.MD5Hash = strings.Trim(*obj.ETag, "\"")
			}

			files = append(files, file)
		}
	}

	return files, nil
}

func listLocalFiles(rootPath string) ([]FileInfo, error) {
	var files []FileInfo

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(rootPath, path)
		if err != nil {
			return err
		}

		relPath = filepath.ToSlash(relPath)

		if shouldIgnoreFile(relPath) {
			return nil
		}

		md5Hash, err := calculateFileMD5(path)
		if err != nil {
			return fmt.Errorf("failed to calculate MD5 for %s: %v", path, err)
		}

		file := FileInfo{
			Path:    path,
			RelPath: relPath,
			Size:    info.Size(),
			MD5Hash: md5Hash,
			IsDir:   false,
		}

		files = append(files, file)
		return nil
	})

	return files, err
}

func filesAreSame(file1, file2 FileInfo) bool {
	if file1.Size != file2.Size {
		return false
	}

	return file1.MD5Hash == file2.MD5Hash
}

func filesAreSameWithMetadataCheck(ctx context.Context, s3Client *s3.Client, localFile, s3File FileInfo, bucket string) bool {
	if localFile.Size != s3File.Size {
		return false
	}

	if localFile.MD5Hash == s3File.MD5Hash {
		return true
	}

	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s3File.Path),
	}

	headResult, headErr := s3Client.HeadObject(ctx, headInput)
	if headErr == nil && headResult.Metadata != nil {
		if storedMD5, exists := headResult.Metadata["local-md5"]; exists {
			return localFile.MD5Hash == storedMD5
		}
	}

	return localFile.MD5Hash == s3File.MD5Hash
}

type downloadSyncTask struct {
	file       FileInfo
	bucket     string
	destPath   string
	downloader *manager.Downloader
}

func downloadFiles(ctx context.Context, s3Client *s3.Client, bucket string, files []FileInfo, result *SyncResult) error {
	downloader := manager.NewDownloader(s3Client)

	var tasks []downloadSyncTask
	for _, file := range files {
		destPath := filepath.Join(destination, filepath.FromSlash(file.RelPath))
		tasks = append(tasks, downloadSyncTask{
			file:       file,
			bucket:     bucket,
			destPath:   destPath,
			downloader: downloader,
		})
	}

	var mutex sync.Mutex

	return runWorkerPool(tasks, maxWorkers, func(task downloadSyncTask) error {
		if dryRun {
			logInfo("Would download: %s\n", task.file.RelPath)
			mutex.Lock()
			result.Downloaded = append(result.Downloaded, task.file.RelPath)
			mutex.Unlock()
			return nil
		}

		destDir := filepath.Dir(task.destPath)
		if err := os.MkdirAll(destDir, 0755); err != nil {
			mutex.Lock()
			result.Errors = append(result.Errors, fmt.Sprintf("Failed to create directory %s: %v", destDir, err))
			mutex.Unlock()
			return nil // Continue processing other files instead of stopping
		}

		if err := downloadSingleFile(ctx, task.downloader, task.bucket, task.file.Path, task.destPath); err != nil {
			mutex.Lock()
			result.Errors = append(result.Errors, fmt.Sprintf("Failed to download %s: %v", task.file.RelPath, err))
			mutex.Unlock()
			return nil // Continue processing other files instead of stopping
		}

		logInfo("Downloaded: %s\n", task.file.RelPath)
		mutex.Lock()
		result.Downloaded = append(result.Downloaded, task.file.RelPath)
		mutex.Unlock()
		return nil
	})
}

type uploadSyncTask struct {
	file     FileInfo
	bucket   string
	s3Key    string
	uploader *manager.Uploader
}

func uploadFiles(ctx context.Context, s3Client *s3.Client, bucket, prefix string, files []FileInfo, result *SyncResult) error {
	uploader := manager.NewUploader(s3Client)

	var tasks []uploadSyncTask
	for _, file := range files {
		s3Key := prefix + file.RelPath
		tasks = append(tasks, uploadSyncTask{
			file:     file,
			bucket:   bucket,
			s3Key:    s3Key,
			uploader: uploader,
		})
	}

	var mutex sync.Mutex

	return runWorkerPool(tasks, maxWorkers, func(task uploadSyncTask) error {
		if dryRun {
			logInfo("Would upload: %s\n", task.file.RelPath)
			mutex.Lock()
			result.Uploaded = append(result.Uploaded, task.file.RelPath)
			mutex.Unlock()
			return nil
		}

		if err := uploadSingleFile(ctx, task.uploader, task.bucket, task.s3Key, task.file.Path); err != nil {
			mutex.Lock()
			result.Errors = append(result.Errors, fmt.Sprintf("Failed to upload %s: %v", task.file.RelPath, err))
			mutex.Unlock()
			return nil // Continue processing other files instead of stopping
		}

		logInfo("Uploaded: %s\n", task.file.RelPath)
		mutex.Lock()
		result.Uploaded = append(result.Uploaded, task.file.RelPath)
		mutex.Unlock()
		return nil
	})
}

func deleteLocalFiles(files []FileInfo, result *SyncResult) error {
	for _, file := range files {
		if dryRun {
			logInfo("Would delete local file: %s\n", file.RelPath)
			result.Deleted = append(result.Deleted, file.RelPath)
			continue
		}

		if err := os.Remove(file.Path); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("Failed to delete local file %s: %v", file.RelPath, err))
			continue
		}

		logInfo("Deleted local file: %s\n", file.RelPath)
		result.Deleted = append(result.Deleted, file.RelPath)
	}
	return nil
}

func deleteS3Files(ctx context.Context, s3Client *s3.Client, bucket string, files []FileInfo, result *SyncResult) error {
	for _, file := range files {
		if dryRun {
			logInfo("Would delete S3 file: %s\n", file.RelPath)
			result.Deleted = append(result.Deleted, file.RelPath)
			continue
		}

		_, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(file.Path),
		})

		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("Failed to delete S3 file %s: %v", file.RelPath, err))
			continue
		}

		logInfo("Deleted S3 file: %s\n", file.RelPath)
		result.Deleted = append(result.Deleted, file.RelPath)
	}
	return nil
}

func downloadSingleFile(ctx context.Context, downloader *manager.Downloader, bucket, key, destPath string) error {
	return downloadFileWithParams(ctx, downloader, bucket, key, destPath, false)
}

func uploadSingleFile(ctx context.Context, uploader *manager.Uploader, bucket, key, filePath string) error {
	return uploadFileWithParams(ctx, uploader, bucket, key, filePath, false)
}

func printSyncSummary(result SyncResult) {
	if quiet {
		return
	}

	fmt.Println("\n=== Sync Summary ===")

	if len(result.Uploaded) > 0 {
		fmt.Printf("Uploaded: %d files\n", len(result.Uploaded))
		if verbose {
			for _, file := range result.Uploaded {
				fmt.Printf("  ↑ %s\n", file)
			}
		}
	}

	if len(result.Downloaded) > 0 {
		fmt.Printf("Downloaded: %d files\n", len(result.Downloaded))
		if verbose {
			for _, file := range result.Downloaded {
				fmt.Printf("  ↓ %s\n", file)
			}
		}
	}

	if len(result.Deleted) > 0 {
		fmt.Printf("Deleted: %d files\n", len(result.Deleted))
		if verbose {
			for _, file := range result.Deleted {
				fmt.Printf("  ✗ %s\n", file)
			}
		}
	}

	if len(result.Errors) > 0 {
		fmt.Printf("Errors: %d\n", len(result.Errors))
		for _, err := range result.Errors {
			fmt.Printf("  ⚠ %s\n", err)
		}
	}

	total := len(result.Uploaded) + len(result.Downloaded) + len(result.Deleted)
	if total == 0 && len(result.Errors) == 0 {
		fmt.Println("Directories are already in sync!")
	}
}
