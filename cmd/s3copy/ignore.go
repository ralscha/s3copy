package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	ignore "github.com/sabhiram/go-gitignore"
)

var ignoreMatcher *ignore.GitIgnore

func initializeIgnoreMatcher() error {
	var patterns []string

	if ignorePatterns != "" {
		for pattern := range strings.SplitSeq(ignorePatterns, ",") {
			trimmed := strings.TrimSpace(pattern)
			if trimmed != "" {
				patterns = append(patterns, trimmed)
			}
		}
	}

	if ignoreFile != "" {
		filePatterns, err := readIgnoreFile(ignoreFile)
		if err != nil {
			return fmt.Errorf("failed to read ignore file %s: %v", ignoreFile, err)
		}
		patterns = append(patterns, filePatterns...)
	}

	if len(patterns) > 0 {
		ignoreMatcher = ignore.CompileIgnoreLines(patterns...)
	}

	return nil
}

func readIgnoreFile(filePath string) ([]string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var patterns []string
	for line := range strings.SplitSeq(string(content), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			patterns = append(patterns, trimmed)
		}
	}

	return patterns, nil
}

func shouldIgnoreFile(filePath string) bool {
	if ignoreMatcher == nil {
		return false
	}

	var relativePath string
	if filepath.IsAbs(filePath) {
		if rel, err := filepath.Rel(source, filePath); err == nil && !strings.HasPrefix(rel, "..") {
			relativePath = rel
		} else {
			relativePath = filepath.Base(filePath)
		}
	} else {
		relativePath = filePath
	}

	normalizedPath := strings.ReplaceAll(relativePath, "\\", "/")
	return ignoreMatcher.MatchesPath(normalizedPath)
}
