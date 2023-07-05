package main

import (
	"regexp"
	"strings"
)

var (
	invalidCharsRegex  = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`) // Invalid characters regex pattern
	invalidSuffixRegex = regexp.MustCompile(`[. ]+$`)                // Invalid trailing dots and spaces regex pattern
)

func formatFolderName(name string) string {
	// Remove invalid characters
	name = invalidCharsRegex.ReplaceAllString(name, "")

	// Remove invalid trailing dots and spaces
	name = invalidSuffixRegex.ReplaceAllString(name, "")

	// Trim leading and trailing spaces
	name = strings.TrimSpace(name)

	// Limit length to 255 characters (typical file system limit)
	if len(name) > 255 {
		name = name[:255]
	}

	return name
}
