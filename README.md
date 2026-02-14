# S3 Copy Tool

A CLI tool to copy files between local storage and S3-compatible storage with optional encryption.

## Features

- Copy files and directories between local storage and S3-compatible storage
- Support for glob patterns
- Encryption using ChaCha20-Poly1305 with Argon2 key derivation
- Recursive directory copying
- Gitignore-style file filtering with support for ignore files
- Environment variable configuration

## Installation

```bash
go mod tidy
go build -o s3copy
```

## Configuration

Create a `.env` file in the same directory as the executable:

```env
# S3COPY_ENDPOINT is optional - only needed for S3-compatible services (like OVH, MinIO, etc.)
# Leave empty or omit for AWS S3
S3COPY_ENDPOINT=https://s3.sbg.io.cloud.ovh.net/
S3COPY_ACCESS_KEY=your_access_key_here
S3COPY_SECRET_KEY=your_secret_key_here
# S3COPY_REGION is optional - defaults to us-east-1 if not specified
S3COPY_REGION=us-east-1
# S3COPY_USE_PATH_STYLE is optional - defaults to false. Set to true for MinIO or other services requiring path-style URLs
S3COPY_USE_PATH_STYLE=false
```

You can also specify a custom `.env` file path using the `--env` flag.

## Usage

### Basic Operations

```bash
# Upload a file
./s3copy -s localfile.txt -d s3://mybucket/path/file.txt

# Download a file
./s3copy -s s3://mybucket/path/file.txt -d localfile.txt

# Upload directory recursively
./s3copy -s ./my_folder -d s3://mybucket/backup/ -r

# Download directory
./s3copy -s s3://mybucket/backup/ -d ./restored_files/

# Upload with encryption
./s3copy -s localfile.txt -d s3://mybucket/encrypted_file.txt -e -p mypassword

# Upload with glob pattern
./s3copy -s "*.txt" -d s3://mybucket/text_files/

# List bucket contents
./s3copy --list -b my-bucket --filter "documents/" --detailed
```

### Smart Path Handling

When copying single files (not directories), intelligent path handling is applied:

**Uploading to S3 directory** - When destination ends with `/`, source filename is appended:
```bash
./s3copy -s localfile.txt -d s3://mybucket/documents/
# Results in: s3://mybucket/documents/localfile.txt
```

**Downloading to local directory** - When destination is `.`, `./`, or existing directory, S3 filename is used:
```bash
./s3copy -s s3://mybucket/path/to/file.txt -d ./
# Results in: ./file.txt
```

### Command Line Flags

- `-s, --source`: Source path (local file/directory or s3://bucket/key)
- `-d, --destination`: Destination path (local file/directory or s3://bucket/key)
- `-b, --bucket`: S3 bucket name (required for S3 operations)
- `-e, --encrypt`: Enable encryption/decryption (required for both encrypting and decrypting files)
- `-p, --password`: Encryption password (omit value to prompt interactively)
- `-r, --recursive`: Copy directories recursively
- `-l, --list`: List objects in bucket
- `-f, --filter`: Filter objects by prefix (used with --list)
- `--detailed`: Show detailed information when listing (storage class, ETag, etc.)
- `--env`: Path to .env file (default: ".env")
- `--ignore`: Comma-separated list of patterns to ignore (gitignore syntax)
- `--ignore-file`: Path to file containing ignore patterns (one per line, gitignore syntax)
- `--max-workers`: Maximum number of concurrent workers for uploads/downloads (default: 5)
- `--dry-run`: Show what would be done without actually performing the operations
- `--quiet`: Suppress non-error output
- `--verbose`: Enable verbose output
- `--timeout`: Timeout for operations in seconds (0 for no timeout)
- `--retries`: Number of retry attempts for failed operations (default: 3)
- `--force, --force-overwrite`: Force overwrite files even if they exist with same checksum. By default, existing files with same checksum are skipped (default: false)
- `--sync`: Enable sync mode to make destination directory exactly match source directory (one-way sync)
- `--sync-compare`: Sync compare strategy: `checksum` (default) or `size-time`

## Checksum-Based Skip Optimization

By default, s3copy performs intelligent uploading/downloading by comparing file checksums. This feature helps avoid unnecessary uploads and downloads when files haven't changed.

### How It Works

1. **Local Checksum**: The tool calculates the MD5 checksum of the local file
2. **Remote Check**: It checks if an S3 object exists at the destination path
3. **Comparison**: If the object exists, it compares the remote ETag (MD5) with the local checksum
4. **Skip or Upload**: 
   - **Default behavior**: Files with matching checksums are automatically skipped
   - **With --force flag**: Files are uploaded/downloaded even if checksums match

Use the `--force` flag to bypass checksum checking and always overwrite files. Note that checksum checking is automatically disabled when using encryption.

## Sync Mode

Sync mode ensures that the destination directory looks exactly like the source directory. The source is always treated as the master, and the destination is modified to match it. This feature is ideal for creating and maintaining exact replicas of directories.

Sync mode makes the destination directory exactly match the source directory through one-way synchronization. It compares files by size and checksums, then copies new/updated files and deletes files that don't exist in source.

### Sync Compare Strategies

- `checksum` (default): highest confidence, computes local MD5 and compares against S3 ETag/metadata.
- `size-time`: faster for very large trees, compares file size and modification time metadata (`local-mtime`) to avoid hashing every local file.

Example:

```bash
# Faster sync comparisons for large directory trees
./s3copy --sync --sync-compare size-time -s ./local_folder -d s3://mybucket/backup/
```

### Usage Examples
```bash
# Make S3 bucket exactly match local directory
./s3copy --sync -s ./local_folder -d s3://mybucket/backup/

# Sync with dry run to see what would be done
./s3copy --sync -s ./local_folder -d s3://mybucket/backup/ --dry-run

# Sync with ignore patterns
./s3copy --sync -s ./project -d s3://mybucket/project-backup/ --ignore "*.log,node_modules/,build/"
```

**Important Notes:**

- **One-Way Operation**: Source is always master; destination is modified to match source
- **Destructive Operation**: Files in destination that don't exist in source will be deleted
- **S3 to S3**: Direct S3-to-S3 sync is not supported (use local as intermediary)
- **Safety**: Always test with `--dry-run` first to verify the intended operations
- **Backup**: Consider backing up important data before running sync operations

## File Filtering (Ignore Patterns)

s3copy supports gitignore-style patterns to exclude files and directories. Use `--ignore` for inline patterns or `--ignore-file` to load patterns from a file.

### Pattern Syntax

- `*` - matches any characters except `/`
- `**` - matches any characters including `/`
- `?` - matches a single character except `/`
- `[abc]` - matches any character in brackets
- `!pattern` - negates the pattern (includes files)
- `/pattern` - matches from root directory only
- `pattern/` - matches directories only
- `#` - comments
- Empty lines are ignored

### Usage Example

Create an ignore file (e.g., `.s3ignore`) with patterns (one per line):

```
# Comments are supported
*.log
*.tmp
*.temp

# Ignore directories
node_modules/
build/
dist/
.git/

# Ignore specific files
secret.txt
config.local.json

# Ignore all files starting with dot
.*

# Ignore build artifacts in root only
/build/
/dist/
```

Use it:
```bash
./s3copy -s ./my_project -d s3://backup/my_project -r --ignore-file .gitignore
```

## Encryption

Encryption uses ChaCha20-Poly1305 (authenticated encryption) with Argon2id key derivation (3 iterations, 64 MB memory, 4 threads). Each encrypted file contains: `[32-byte salt][12-byte nonce][encrypted data]`

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
