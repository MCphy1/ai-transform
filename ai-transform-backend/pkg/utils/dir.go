package utils

import (
	"io"
	"os"
	"path"
)

func CreateDirIfNotExists(dirPath ...string) error {
	for _, p := range dirPath {
		dir := p
		ext := path.Ext(p)
		if ext != "" {
			dir = path.Dir(p)
		}
		// 检查文件夹是否存在
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0644)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

// CopyFile 复制文件
func CopyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}

	return nil
}
