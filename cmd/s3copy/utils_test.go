package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{1099511627776, "1.0 TB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatBytes(tt.bytes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTruncateString(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		expected string
	}{
		{"short", 10, "short"},
		{"verylongstring", 10, "verylon..."},
		{"exact", 5, "exact"},
		{"toolong", 5, "to..."},
		{"a", 3, "a"},
		{"abc", 3, "abc"},
		{"abcd", 3, "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := truncateString(tt.input, tt.maxLen)
			assert.Equal(t, tt.expected, result)
			assert.LessOrEqual(t, len(result), tt.maxLen)
		})
	}
}

func TestCalculateFileMD5(t *testing.T) {
	t.Run("CalculateMD5ForFile", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test-md5-*.txt")
		require.NoError(t, err)
		defer func() { _ = os.Remove(tmpFile.Name()) }()

		testContent := "Hello, World!\nThis is a test file.\n"
		_, err = tmpFile.WriteString(testContent)
		require.NoError(t, err)
		closeWithLog(tmpFile, "test file")

		hash, err := calculateFileMD5(tmpFile.Name())
		require.NoError(t, err)
		assert.NotEmpty(t, hash)
		assert.Len(t, hash, 32)
	})

	t.Run("CalculateMD5ForNonExistentFile", func(t *testing.T) {
		_, err := calculateFileMD5("/non/existent/file.txt")
		assert.Error(t, err)
	})
}

func TestLogFunctions(t *testing.T) {
	originalQuiet := quiet
	originalVerbose := verbose
	defer func() {
		quiet = originalQuiet
		verbose = originalVerbose
	}()

	t.Run("logInfo when not quiet", func(t *testing.T) {
		quiet = false
		output := captureStdout(func() {
			logInfo("test message %s", "arg")
		})
		assert.Contains(t, output, "test message arg")
	})

	t.Run("logInfo when quiet", func(t *testing.T) {
		quiet = true
		output := captureStdout(func() {
			logInfo("should not print")
		})
		assert.Empty(t, output)
	})

	t.Run("logVerbose when verbose", func(t *testing.T) {
		verbose = true
		output := captureStdout(func() {
			logVerbose("verbose message %d", 42)
		})
		assert.Contains(t, output, "verbose message 42")
	})

	t.Run("logVerbose when not verbose", func(t *testing.T) {
		verbose = false
		output := captureStdout(func() {
			logVerbose("should not print")
		})
		assert.Empty(t, output)
	})
}

func TestDecodeS3Key(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no encoding needed",
			input:    "simple/path/file.txt",
			expected: "simple/path/file.txt",
		},
		{
			name:     "German umlaut ö",
			input:    "music/Melancholisch%20sch%C3%B6n/song.mp3",
			expected: "music/Melancholisch schön/song.mp3",
		},
		{
			name:     "German umlaut ü",
			input:    "music/M%C3%BCnchen/file.mp3",
			expected: "music/München/file.mp3",
		},
		{
			name:     "German umlaut ä",
			input:    "music/M%C3%A4rchen/file.mp3",
			expected: "music/Märchen/file.mp3",
		},
		{
			name:     "space encoded as %20",
			input:    "path%20with%20spaces/file.txt",
			expected: "path with spaces/file.txt",
		},
		{
			name:     "space encoded as +",
			input:    "path+with+plus/file.txt",
			expected: "path with plus/file.txt",
		},
		{
			name:     "mixed special characters",
			input:    "2raumwohnung/Melancholisch%20sch%C3%B6n/track.mp3",
			expected: "2raumwohnung/Melancholisch schön/track.mp3",
		},
		{
			name:     "already decoded string",
			input:    "music/Melancholisch schön/song.mp3",
			expected: "music/Melancholisch schön/song.mp3",
		},
		{
			name:     "invalid percent encoding - returns original",
			input:    "invalid%ZZencoding",
			expected: "invalid%ZZencoding",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := decodeS3Key(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
