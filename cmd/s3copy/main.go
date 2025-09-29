package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/urfave/cli/v3"
)

var (
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
	forceOverwrite bool
	syncMode       bool
)

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
				Name:        "force",
				Aliases:     []string{"force-overwrite"},
				Usage:       "Force overwrite files even if they exist with same checksum (by default, existing files with same checksum are skipped)",
				Destination: &forceOverwrite,
			},
			&cli.BoolFlag{
				Name:        "sync",
				Usage:       "Sync mode: makes destination directory exactly match source directory (one-way sync)",
				Destination: &syncMode,
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

				if syncMode {
					sourceIsS3 := strings.HasPrefix(source, "s3://")
					destIsS3 := strings.HasPrefix(destination, "s3://")

					if sourceIsS3 && destIsS3 {
						return ctx, fmt.Errorf("S3 to S3 sync is not supported")
					}

					if !sourceIsS3 && !destIsS3 {
						return ctx, fmt.Errorf("at least one of source or destination must be S3 in sync mode")
					}

					if !sourceIsS3 {
						if info, err := os.Stat(source); err != nil {
							return ctx, fmt.Errorf("source directory does not exist: %v", err)
						} else if !info.IsDir() {
							return ctx, fmt.Errorf("source must be a directory in sync mode")
						}
					}

					if !destIsS3 {
						if info, err := os.Stat(destination); err != nil {
							if !os.IsNotExist(err) {
								return ctx, fmt.Errorf("error checking destination: %v", err)
							}
						} else if !info.IsDir() {
							return ctx, fmt.Errorf("destination must be a directory in sync mode")
						}
					}
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

	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}

	if syncMode {
		if err := syncDirectories(ctx); err != nil {
			return fmt.Errorf("error syncing directories: %v", err)
		}
		logInfo("Sync operation completed successfully!\n")
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

	if sourceIsS3 {
		if err := downloadFromS3(ctx); err != nil {
			return fmt.Errorf("error downloading from S3: %v", err)
		}
	} else {
		if err := uploadToS3(ctx); err != nil {
			return fmt.Errorf("error uploading to S3: %v", err)
		}
	}

	logInfo("Copy operation completed successfully!\n")
	return nil
}
