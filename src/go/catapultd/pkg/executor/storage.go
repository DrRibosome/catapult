package executor

import (
	"bufio"
	"cloud.google.com/go/storage"
	"context"
	"io"
	"os"
)

const (
	logBucket = "temp.levelz.io"
)

func uploadFile(filePath, gcsKey string, client *storage.Client) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	obj := client.Bucket(logBucket).Object(gcsKey)
	writer := obj.NewWriter(context.Background())

	_, err = io.Copy(writer, reader)
	if err != nil {
		return err
	}

	return writer.Close()
}
