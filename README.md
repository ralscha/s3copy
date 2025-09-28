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

### Basic Commands

Upload a file to S3 storage:
```bash
./s3copy -s localfile.txt -d s3://mybucket/path/file.txt
```

Download a file from S3 storage:
```bash
./s3copy -s s3://mybucket/path/file.txt -d localfile.txt
```

### Advanced Usage

Upload with encryption:
```bash
./s3copy -s localfile.txt -d s3://mybucket/encrypted_file.txt -e
```

Upload with password provided:
```bash
./s3copy -s localfile.txt -d s3://mybucket/encrypted_file.txt -e -p mypassword
```

Upload directory recursively:
```bash
./s3copy -s ./my_folder -d s3://mybucket/backup/ -r
```

Upload with glob pattern:
```bash
./s3copy -s "*.txt" -d s3://mybucket/text_files/
```

Upload directory with encryption:
```bash
./s3copy -s ./my_folder -d s3://mybucket/encrypted_backup/ -r -e
```

Upload with checksum checking (skip if file already exists with same content):
```bash
./s3copy -s ./my_folder -d s3://mybucket/backup/ -r --skip-existing
```

Download encrypted file:
```bash
./s3copy -s s3://mybucket/encrypted_file.txt -d decrypted_file.txt -e
```

Dry run upload:
```bash
./s3copy -s ./my_folder -d s3://mybucket/backup/ -r --dry-run
```

Quiet operation:
```bash
./s3copy -s file.txt -d s3://mybucket/file.txt --quiet
```

### Single Copy Mode

When copying single files (not directories), s3copy provides intelligent path handling:

#### Upload to S3 Directory
When the S3 destination ends with `/`, the source filename is automatically appended:
```bash
# Upload localfile.txt to s3://mybucket/documents/localfile.txt
./s3copy -s localfile.txt -d s3://mybucket/documents/

# Upload report.pdf to s3://mybucket/reports/2023/report.pdf
./s3copy -s report.pdf -d s3://mybucket/reports/2023/
```

#### Download to Local Directory
When downloading to a local directory (`.`, `./`, or existing directory), the S3 key filename is used:
```bash
# Download s3://mybucket/path/to/file.txt to ./file.txt
./s3copy -s s3://mybucket/path/to/file.txt -d ./

# Download s3://mybucket/documents/report.pdf to /home/user/downloads/report.pdf
./s3copy -s s3://mybucket/documents/report.pdf -d /home/user/downloads/
```

This behavior only applies to single file operations. For directory operations (`-r` flag), the full path structure is preserved.

Verbose output with retries:
```bash
./s3copy -s large_file.zip -d s3://mybucket/large_file.zip --verbose --retries 5
```

Download entire bucket prefix:
```bash
./s3copy -s s3://mybucket/backup/ -d ./restored_files/
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
- `--skip-existing`: Check if file already exists with same checksum before uploading/downloading. If true, skip the operation (default: false)

## Checksum-Based Upload Optimization

The `--skip-existing` flag enables intelligent uploading/downloading by comparing file checksums before uploading/downloading. This feature helps avoid unnecessary uploads and downloads when files haven't changed.

### How It Works

1. **Local Checksum**: The tool calculates the MD5 checksum of the local file
2. **Remote Check**: It checks if an S3 object exists at the destination path
3. **Comparison**: If the object exists, it compares the remote ETag (MD5) with the local checksum
4. **Skip or Upload**: Files with matching checksums are skipped; only changed files are uploaded

### Limitations

- **Encryption**: Checksum checking is disabled when using encryption (`--encrypt`)

## File Filtering (Ignore Patterns)

s3copy supports gitignore-style patterns to exclude files and directories from upload operations. This is particularly useful for skipping temporary files, build artifacts, or sensitive data.

### Using Command Line Patterns

```bash
# Ignore specific file types
./s3copy -s ./project -d s3://backup/project -r --ignore "*.log,*.tmp,node_modules/"

# Ignore multiple patterns
./s3copy -s ./project -d s3://backup/project -r --ignore "*.log,build/,dist/,.git/"
```

### Using Ignore Files

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

Then use it:

```bash
./s3copy -s ./project -d s3://backup/project -r --ignore-file .s3ignore
```

### Ignore Pattern Syntax

The tool supports standard gitignore syntax:

- `*` - matches any number of characters except `/`
- `**` - matches any number of characters including `/`
- `?` - matches a single character except `/`
- `[abc]` - matches any character in the brackets
- `!pattern` - negates the pattern (includes files that would otherwise be ignored)
- `/pattern` - matches from the root directory only
- `pattern/` - matches directories only
- Lines starting with `#` are comments
- Empty lines are ignored

### Examples with Ignore Patterns

Upload a project while ignoring common build artifacts:
```bash
./s3copy -s ./my_project -d s3://backup/my_project -r --ignore "node_modules/,*.log,build/,dist/"
```

Upload with a comprehensive ignore file:
```bash
./s3copy -s ./my_project -d s3://backup/my_project -r --ignore-file .gitignore
```

## Encryption Details

The tool uses the following encryption algorithms:

- **Cipher**: ChaCha20-Poly1305 (authenticated encryption)
- **Key Derivation**: Argon2id with the following parameters:
  - Time: 3 iterations
  - Memory: 64 MB
  - Threads: 4
  - Key length: 32 bytes
- **Salt**: 32 random bytes per file
- **Nonce**: 12 random bytes per file

Each encrypted file contains: `[32-byte salt][12-byte nonce][encrypted data]`

## Examples

### Backup a project directory
```bash
./s3copy -s ./my_project -d s3://backups/my_project_$(date +%Y%m%d) -r -e
```

### Restore a backup
```bash
./s3copy -s s3://backups/my_project_20231201 -d ./restored_project -e
```

### Backup specific file types
```bash
./s3copy -s "*.go" -d s3://code-backup/go-files/ -e
```

### List and explore bucket contents
```bash
# List all objects in a bucket
./s3copy --list -b my-bucket

# List objects with specific prefix (like a folder)
./s3copy --list -b my-bucket --filter "documents/"

# List with detailed information including storage class and ETags
./s3copy --list -b my-bucket --detailed

```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


