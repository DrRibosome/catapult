package main

import (
	"catapultd/pkg/executor"
	"catapultd/pkg/gen/api"
	"cloud.google.com/go/storage"
	"context"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"os"
)

// catapult executor handles launching python tasks
func main() {
	var serverAddr string
	var workDir string
	var clientID string
	flag.StringVar(&serverAddr, "server", "", "catapultd address")
	flag.StringVar(&workDir, "work", "", "work directory to use for task disk access")
	flag.StringVar(&clientID, "job", "", "job id")
	flag.Parse()

	executorID := os.Getenv("K8S_POD_NAME")
	if executorID == "" {
		fmt.Fprintln(os.Stderr, "expected pod name via env var: K8S_POD_NAME")
		os.Exit(1)
	}

	if workDir != "" {
		if err := os.MkdirAll(workDir, os.ModePerm); err != nil {
			panic(err)
		}
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	storageClient, err := storage.NewClient(context.Background())
	if err != nil {
		logger.Fatal("failed to create GCS client", zap.Error(err))
	}

	logger.Info("starting",
		zap.String("server", serverAddr),
		zap.String("workdir", workDir),
		zap.String("client", clientID))

	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		logger.Fatal("failed to connect", zap.Error(err))
	}
	defer conn.Close()

	config := executor.Config{
		ID:      executorID,
		WorkDir: workDir,
		JobID:   clientID,
	}

	client := api.NewExecutorServiceClient(conn)
	stream, err := client.Tasks(context.Background())
	if err != nil {
		logger.Fatal("failed to create stream", zap.Error(err))
	}

	// handle k8s SIGTERM shutdown signal gracefully
	/*notify := make(chan os.Signal, 1)
	signal.Notify(notify, syscall.SIGTERM)
	go func() {
		select {
		case <-notify:
			exe.Shutdown()
		}
	}()*/

	/*var finalErr error
	for {
		exe := executor.New(config, storageClient, stream, logger)
		finalErr = exe.Run()
		if finalErr != io.EOF {
			break
		}
		logger.Info("restarting excutor", zap.Error(finalErr))
	}*/
	exe := executor.New(config, storageClient, stream, logger)
	finalErr := exe.Run()
	logger.Info("exiting", zap.Error(finalErr))
}
