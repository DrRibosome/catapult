package podwatcher

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"time"
)

type PodWatcher struct {
	namespace string
	clientset *kubernetes.Clientset
	queue     workqueue.RateLimitingInterface
	indexer   cache.Indexer
	informer  cache.Controller
	logger    *zap.Logger

	handler PodHandler
}

type PodHandler interface {
	SendPodEvent(event Event)
}

func New(namespace string, clientset *kubernetes.Clientset, logger *zap.Logger, handler PodHandler) *PodWatcher {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	listWatch := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.LabelSelector = "app=catapult-runner"
			pods := clientset.CoreV1().Pods(namespace)
			return pods.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.LabelSelector = "app=catapult-runner"
			pods := clientset.CoreV1().Pods(namespace)
			return pods.Watch(options)
		},
	}

	indexer, informer := cache.NewIndexerInformer(listWatch, &corev1.Pod{}, 60, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err != nil {
				return
			}

			//logger.Info("add", zap.String("key", key))
			queue.Add(key)

			pod := obj.(*corev1.Pod)
			handler.SendPodEvent(&PodAdded{Pod: pod})
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			/*key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				logger.Info("update", zap.String("key", key))
				queue.Add(key)
			}*/
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*corev1.Pod)
			handler.SendPodEvent(&PodDeleted{Pod: pod})

			// IndexerInformer uses a delta queue, therefore for deletes we have to use this
			// key function.
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err != nil {
				return
			}

			//logger.Info("delete", zap.String("key", key))
			queue.Forget(key)
		},
	}, cache.Indexers{})

	return &PodWatcher{
		namespace: namespace,
		clientset: clientset,
		queue:     queue,
		indexer:   indexer,
		informer:  informer,
		logger:    logger,

		handler: handler,
	}
}

func (c *PodWatcher) runWorker() {
	for c.processNextItem() {
	}
}

func (c *PodWatcher) processNextItem() bool {
	// Wait until there is a new item in the working queue
	key, quit := c.queue.Get()
	if quit {
		return false
	}

	// Tell the queue that we are done with processing this key. This unblocks the key for other workers
	// This allows safe parallel processing because two pods with the same key are never processed in
	// parallel.
	defer c.queue.Done(key)

	// Invoke the method containing the business logic
	err := c.syncKey(key.(string))
	// Handle the error if something went wrong during the execution of the business logic
	c.handleErr(err, key)

	if err == nil {
		// no issue processing the key, check back later
		c.queue.AddAfter(key, 10*time.Second) // TODO: add jitter
	}

	return true
}

// syncToStdout is the business logic of the controller. In this controller it simply prints
// information about the pod to stdout. In case an error happened, it has to simply return the error.
// The retry logic should not be part of the business logic.
func (c *PodWatcher) syncKey(key string) error {
	obj, exists, err := c.indexer.GetByKey(key)
	if err != nil {
		return errors.Errorf("Fetching object with key %s from store failed with %v", key, err)
	}

	if !exists {
		c.queue.Forget(key)
		return nil
	}

	pod := obj.(*corev1.Pod)
	//c.logger.Info("sync", zap.String("name", pod.Name))

	result := make(chan bool, 1)
	c.handler.SendPodEvent(&ShouldDelete{
		Pod:    pod,
		Result: result,
	})
	shouldDelete := <-result

	if shouldDelete {
		pods := c.clientset.CoreV1().Pods(c.namespace)
		err := pods.Delete(pod.Name, nil)
		c.logger.Info("delete pod",
			zap.String("key", key),
			zap.Error(err))

		if err != nil {
			return err
		}

		// succesfully deleted
		c.queue.Forget(key)
		return nil
	}

	return nil
}

// handleErr checks if an error happened and makes sure we will retry later.
func (c *PodWatcher) handleErr(err error, key interface{}) {
	if err == nil {
		// Forget about the #AddRateLimited history of the key on every successful synchronization.
		// This ensures that future processing of updates for this key is not delayed because of
		// an outdated error history.
		c.queue.Forget(key)
		return
	}

	// This controller retries 5 times if something goes wrong. After that, it stops trying.
	if c.queue.NumRequeues(key) < 5 {
		c.logger.Sugar().Infof("Error syncing pod %v: %v", key, err)

		// Re-enqueue the key rate limited. Based on the rate limiter on the
		// queue and the re-enqueue history, the key will be processed later again.
		c.queue.AddRateLimited(key)
		return
	}

	c.queue.Forget(key)
	// Report to an external entity that, even after several retries, we could not successfully process this key
	//runtime.HandleError(err)
	c.logger.Sugar().Infof("Dropping pod %q out of the queue: %v", key, err)
}

func (c *PodWatcher) Run() error {
	defer c.queue.ShutDown()

	stopCh := make(chan struct{})
	go c.informer.Run(stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		return errors.New("Timed out waiting for caches to sync")
	}

	threadiness := 1
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	return nil
}
