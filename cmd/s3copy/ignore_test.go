package main

import (
	"os"
	"testing"

	ignore "github.com/sabhiram/go-gitignore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitializeIgnoreMatcher(t *testing.T) {
	restore := preserveGlobalVars()
	defer restore()

	t.Run("no patterns", func(t *testing.T) {
		ignorePatterns = ""
		ignoreFile = ""
		err := initializeIgnoreMatcher()
		assert.NoError(t, err)
		assert.Nil(t, ignoreMatcher)
	})

	t.Run("with patterns", func(t *testing.T) {
		ignorePatterns = "*.tmp,*.log"
		ignoreFile = ""
		err := initializeIgnoreMatcher()
		assert.NoError(t, err)
		assert.NotNil(t, ignoreMatcher)

		assert.True(t, ignoreMatcher.MatchesPath("file.tmp"))
		assert.True(t, ignoreMatcher.MatchesPath("file.log"))
		assert.False(t, ignoreMatcher.MatchesPath("file.txt"))
	})

	t.Run("with ignore file", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "ignore_test")
		require.NoError(t, err)
		defer func() { _ = os.Remove(tempFile.Name()) }()

		_, err = tempFile.WriteString("*.bak\n# comment\n*.old\n")
		require.NoError(t, err)
		_ = tempFile.Close()

		ignorePatterns = ""
		ignoreFile = tempFile.Name()
		err = initializeIgnoreMatcher()
		assert.NoError(t, err)
		assert.NotNil(t, ignoreMatcher)

		assert.True(t, ignoreMatcher.MatchesPath("file.bak"))
		assert.True(t, ignoreMatcher.MatchesPath("file.old"))
		assert.False(t, ignoreMatcher.MatchesPath("file.txt"))
		assert.False(t, ignoreMatcher.MatchesPath("comment"))
	})
}

func TestReadIgnoreFile(t *testing.T) {
	t.Run("valid file", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "ignore_test")
		require.NoError(t, err)
		defer func() { _ = os.Remove(tempFile.Name()) }()

		content := "*.tmp\n\n# comment\n*.log\n"
		_, err = tempFile.WriteString(content)
		require.NoError(t, err)
		_ = tempFile.Close()

		patterns, err := readIgnoreFile(tempFile.Name())
		assert.NoError(t, err)
		assert.Equal(t, []string{"*.tmp", "*.log"}, patterns)
	})

	t.Run("nonexistent file", func(t *testing.T) {
		_, err := readIgnoreFile("/nonexistent/file")
		assert.Error(t, err)
	})
}

func TestShouldIgnoreFile(t *testing.T) {
	restore := preserveGlobalVars()
	defer restore()

	t.Run("no matcher", func(t *testing.T) {
		ignoreMatcher = nil
		assert.False(t, shouldIgnoreFile("file.txt"))
	})

	t.Run("with matcher", func(t *testing.T) {
		patterns := []string{"*.tmp", "temp/*"}
		ignoreMatcher = ignore.CompileIgnoreLines(patterns...)

		source = "/tmp"

		assert.True(t, shouldIgnoreFile("/tmp/file.tmp"))
		assert.True(t, shouldIgnoreFile("temp/file.txt"))
		assert.False(t, shouldIgnoreFile("/tmp/file.txt"))
	})

	t.Run("relative path", func(t *testing.T) {
		patterns := []string{"*.tmp"}
		ignoreMatcher = ignore.CompileIgnoreLines(patterns...)
		source = "/tmp"

		assert.True(t, shouldIgnoreFile("file.tmp"))
		assert.False(t, shouldIgnoreFile("file.txt"))
	})

	t.Run("absolute path outside source", func(t *testing.T) {
		patterns := []string{"*.tmp"}
		ignoreMatcher = ignore.CompileIgnoreLines(patterns...)
		source = "/tmp"

		assert.True(t, shouldIgnoreFile("/other/file.tmp"))
		assert.False(t, shouldIgnoreFile("/other/file.txt"))
	})
}
