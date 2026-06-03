package runner

import (
	"crypto/md5"
	"fmt"
	"os"
)

// GetMD5 calculates MD5 hash of a file for caching purposes
func GetMD5(filePath string) (string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file: %w", err)
	}

	hash := md5.Sum(data)
	return fmt.Sprintf("%x", hash), nil
}
