package main

import (
	"catapultd/pkg/catapult"
	"catapultd/pkg/gen/api"
	"catapultd/pkg/podbackend"
	"catapultd/pkg/podwatcher"
	"catapultd/pkg/service/clientsvc"
	"catapultd/pkg/service/exesvc"
	"catapultd/pkg/util/launcher"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"net"
	"time"
)

func main() {
	var kubeConfigPath string
	namespace := "default"
	flag.StringVar(&kubeConfigPath, "kubeconfig", "", "kube config path")
	flag.Parse()

	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	clientset, err := buildClientset(kubeConfigPath)
	if err != nil {
		logger.Fatal("failed to create k8s clientset", zap.Error(err))
	}

	podBackend := podbackend.New(namespace, clientset, logger)
	watcher := podwatcher.New(namespace, clientset, logger, podBackend)

	planner := catapult.NewPlanner(podBackend, logger)
	server := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 60 * time.Second,
		}),
	)
	executorService := exesvc.NewExecutorService(planner, logger)
	gw := clientsvc.NewClientService(planner, logger.With(zap.String("sub", "gw")))

	grpcServer := launcher.AsRunner(func() error {
		api.RegisterExecutorServiceServer(server, executorService)
		api.RegisterTaskGatewayServer(server, gw)
		port := 9998
		logger.Info("starting")
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			logger.Fatal("failed to listen", zap.Error(err))
		}

		finalErr := server.Serve(lis)
		logger.Fatal("exiting", zap.Error(finalErr))
		return err
	})

	launcher.RunAll(watcher, podBackend, planner, grpcServer)
}

func buildClientset(kubeConfigPath string) (*kubernetes.Clientset, error) {
	masterURL := ""
	config, err := clientcmd.BuildConfigFromFlags(masterURL, kubeConfigPath)
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}
