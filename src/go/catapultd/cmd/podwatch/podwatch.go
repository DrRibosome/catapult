package main

import (
	"catapultd/pkg/podwatcher"
	"fmt"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	// import to fix: No Auth Provider found for name "gcp"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	kubeConfigPath := "/Users/crawford/.kube/config"
	namespace := "default"

	clientset, err := buildClientset(kubeConfigPath)
	if err != nil {
		panic(err)
	}

	handler := new(Handler)

	watcher := podwatcher.New(namespace, clientset, logger, handler)
	watcher.Run()
}

type Handler struct {
}

func (h *Handler) Added(pod *corev1.Pod) {
	fmt.Println("detect add:", pod.Name)
}

func (h *Handler) ShouldDelete(pod *corev1.Pod) bool {
	fmt.Println("should delete?", pod.Name)
	return true
}

func buildClientset(kubeConfigPath string) (*kubernetes.Clientset, error) {
	masterURL := ""
	config, err := clientcmd.BuildConfigFromFlags(masterURL, kubeConfigPath)
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}
