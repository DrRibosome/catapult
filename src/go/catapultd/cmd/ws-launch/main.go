package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var masterURL, kubeConfigPath string
	flag.StringVar(&kubeConfigPath, "config", "", "kube config path")
	flag.Parse()

	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	logger.Info("starting")

	config, err := clientcmd.BuildConfigFromFlags(masterURL, kubeConfigPath)
	if err != nil {
		logger.Fatal("failed to build rest config", zap.Error(err))
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Fatal("failed to create k8s clientset", zap.Error(err))
	}

	scheduler := &Scheduler{
		logger:    logger,
		clientset: clientset,
		namespace: "default",
	}
	go scheduler.Run()

	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	var counter int64

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, req *http.Request) {
		index := atomic.AddInt64(&counter, 1)
		logger := logger.With(zap.Int64("conn", index))
		logger.Info("new request")

		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			logger.Warn("failed to upgrade", zap.Error(err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		go handleConn(conn, clientset, logger)
	})

	err = http.ListenAndServe(":9999", mux)
	logger.Info("shutting down", zap.Error(err))
}

type Resources struct {
	CPUs float64
	Mem  float64
	Disk float64
}

type Request struct {
	TaskID    string `json:"task_id"`
	Resources Resources
	Image     string
	// InputData gives google cloud storage path containing pickled data to run
	InputData string `json:"input_data"`
	// OutputData locatin to store results
	OutputData string `json:"output_data"`
}

type Response struct {
	ID string `json:"id"`
}

func handleConn(ws *websocket.Conn, clientset *kubernetes.Clientset, logger *zap.Logger) {
	//defer ws.Close()

	ws.SetPingHandler(func(data string) error {
		logger.Info("ping", zap.String("data", data))
		return nil
	})

	requests := make(chan Request, 1000)
	//defer close(requests)

	// write loop
	go func() {
		timer := time.NewTicker(1 * time.Second)
		defer timer.Stop()
		for {
			select {
			case req := <-requests:
				logger.Sugar().Infof("handling request: %v", req)
				job, err := launchTask(req, clientset)
				if err != nil {
					logger.Error("failed to launch task", zap.Error(err))
				}
				logger.Sugar().Infof("job: %v", job)

			case <-timer.C:
				err := ws.WriteControl(websocket.PingMessage, []byte("misc"), time.Now().Add(time.Second*5))
				if err != nil {
					logger.Warn("failed to write control", zap.Error(err))
					return
				}

				/*select {
				case req := <-requests:
					logger.Info("write response", zap.String("id", req.TaskID))
					resp := Response{
						ID: req.TaskID,
					}
					err = ws.WriteJSON(resp)
					if err != nil {
						logger.Warn("failed to write msg", zap.Error(err))
						return
					}
				default:
				}*/
			}
		}
	}()

	for {
		msgType, p, err := ws.ReadMessage()
		if err != nil {
			logger.Warn("failed to read msg", zap.Error(err))
			return
		}

		if msgType != websocket.TextMessage {
			logger.Warn("unexpected message type", zap.Int("msg-type", msgType))
			return
		}

		var req Request
		dec := json.NewDecoder(bytes.NewReader(p))
		err = dec.Decode(&req)
		if err != nil {
			logger.Warn("failed to decode", zap.Error(err))
			return
		}

		logger.Sugar().Infof("recv: %v", string(p))
		requests <- req
	}
}

type Scheduler struct {
	logger    *zap.Logger
	clientset *kubernetes.Clientset
	namespace string

	launchedJobs chan *batchv1.Job
}

func (s *Scheduler) Run() error {
	wg := new(sync.WaitGroup)
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := s.watchLoop()
		s.logger.Info("watch loop shutdown", zap.Error(err))
	}()

	go func() {
		defer wg.Done()
		err := s.watchPods()
		s.logger.Info("watch pods loop shutdown", zap.Error(err))
	}()

	wg.Wait()
	return nil
}

func (s *Scheduler) watchPods() error {
	pods := s.clientset.CoreV1().Pods(s.namespace)
	watcher, err := pods.Watch(metav1.ListOptions{
		LabelSelector: "type=catapult",
		Watch:         true,
	})
	if err != nil {
		return err
	}

	for event := range watcher.ResultChan() {
		//s.logger.Sugar().Infof("raw event: %v", event)
		switch e := event.Object.(type) {
		case *corev1.Pod:
			s.logger.Info("pod",
				zap.String("event", string(event.Type)),
				zap.String("name", string(e.Name)),
				zap.String("phase", string(e.Status.Phase)),
				zap.String("status", e.Status.String()),
			)
		}
	}

	return nil
}

func (s *Scheduler) watchLoop() error {
	namespace := "default"
	batch := s.clientset.BatchV1().Jobs(namespace)
	watcher, err := batch.Watch(metav1.ListOptions{
		LabelSelector: "type=catapult",
		Watch:         true,
	})
	if err != nil {
		return err
	}

	for event := range watcher.ResultChan() {
		//s.logger.Sugar().Infof("raw event: %v", event)
		switch e := event.Object.(type) {
		case *batchv1.Job:
			logger := s.logger.With(
				zap.String("event", string(event.Type)),
				zap.String("name", e.Name),
				zap.Int32("active", e.Status.Active),
				zap.Int32("succeeded", e.Status.Succeeded),
				zap.Int32("failed", e.Status.Failed),
			)

			if startTime := e.Status.StartTime; startTime != nil {
				logger = logger.With(zap.Time("start", startTime.Time))
			}

			if completeTime := e.Status.CompletionTime; completeTime != nil {
				logger = logger.With(zap.Time("fin", completeTime.Time))
			}

			logger.Info("job")
		}
	}

	return nil
}

func launchTask(req Request, clientset *kubernetes.Clientset) (*batchv1.Job, error) {
	namespace := "default"

	batch := clientset.BatchV1().Jobs(namespace)

	c := corev1.Container{
		Name:  "test-job-container",
		Image: req.Image,
		Command: []string{
			"python",
		},
		Args: []string{
			"-m", "catapult.runner",
			"--source", req.InputData,
			"--target", req.OutputData,
		},
		ImagePullPolicy: corev1.PullAlways,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse(fmt.Sprintf("%v", req.Resources.CPUs)),
			},
		},

		// mount secrets
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "google-cloud-key",
				ReadOnly:  true,
				MountPath: "/var/secrets/google",
			},
		},
		Env: []corev1.EnvVar{
			// NOTE: the key json here depends on the file name used to upload
			// the secret initially. For instance, if you used:
			// `--from-file=catapult-worker-key.json` then you'd neet to point
			// the google cloud credentials to `$mount/catapult-worker-key.json`
			{Name: "GOOGLE_APPLICATION_CREDENTIALS", Value: "/var/secrets/google/catapult-worker-key.json"},
		},
	}

	var ttlSeconds int32 = 30 * 60 // 30 min
	var parallelism int32 = 1

	secret := corev1.SecretVolumeSource{
		SecretName: "catapult-key",
	}

	job, err := batch.Create(&batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Job",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "test-job-",
			Labels: map[string]string{
				"type": "catapult",
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-job-spec",
					Namespace:    namespace,
					Labels: map[string]string{
						"type": "catapult",
					},
				},
				Spec: corev1.PodSpec{
					Containers:    []corev1.Container{c},
					RestartPolicy: corev1.RestartPolicyOnFailure,

					Volumes: []corev1.Volume{
						// secrets, as created by:
						// kubectl create secret generic catapult-key --from-file=catapult-worker-key.json
						{Name: "google-cloud-key", VolumeSource: corev1.VolumeSource{Secret: &secret}},
					},
				},
			},
			TTLSecondsAfterFinished: &ttlSeconds,
			Parallelism:             &parallelism,
		},
	})

	return job, err
}
