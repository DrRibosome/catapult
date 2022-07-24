package podbackend

import (
	"catapultd/pkg/catapult"
	"catapultd/pkg/podwatcher"
	"catapultd/pkg/util/k8sutil"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
)

type PodBackend struct {
	trackers    map[catapult.ClientID]*Tracker // executor trackers
	schedulerID string
	namespace   string
	clientset   *kubernetes.Clientset
	logger      *zap.Logger

	incomingPodEvents     chan podwatcher.Event
	incomingBackendEvents chan catapult.BackendEvent
	internalEvents        chan internalEvent
}

func New(namespace string, clientset *kubernetes.Clientset, logger *zap.Logger) *PodBackend {
	return &PodBackend{
		trackers:              make(map[catapult.ClientID]*Tracker),
		schedulerID:           uuid.New().String(),
		namespace:             namespace,
		clientset:             clientset,
		logger:                logger,
		incomingPodEvents:     make(chan podwatcher.Event, 1),
		incomingBackendEvents: make(chan catapult.BackendEvent, 1),
		internalEvents:        make(chan internalEvent, 1),
	}
}

func (p *PodBackend) Run() error {
	for {
		select {
		case event := <-p.incomingPodEvents:
			p.handlePodEvent(event)

		case event := <-p.incomingBackendEvents:
			p.handleBackendEvent(event)

		case event := <-p.internalEvents:
			p.handleInternalEvent(event)
		}
	}
}

func (p *PodBackend) handleInternalEvent(event internalEvent) {
	switch event := event.(type) {
	case *launchFinished:
		tracker, ok := p.trackers[event.ClientID]
		if !ok {
			return
		}

		// remove any pods that failed to launch, as we'll never see those
		numFailed := event.NumSubmitted - event.NumLaunched
		tracker.NumLaunching -= numFailed

		if event.Err != nil {
			tracker.NumLaunchErrors++
		} else {
			tracker.NumLaunchErrors = 0
		}

		p.logger.Info("launch finished",
			zap.String("client", event.ClientID.String()),
			zap.Int("launching", tracker.NumLaunching),
			zap.Int("failed", numFailed),
			zap.Int("errs", tracker.NumLaunchErrors),
			zap.Error(event.Err))
	}
}

func (p *PodBackend) handlePodEvent(event podwatcher.Event) {
	switch event := event.(type) {
	case *podwatcher.ShouldDelete:
		clientID, err := k8sutil.ExtractClientID(event.Pod)
		if err != nil {
			p.logger.Warn("missing client id", zap.Error(err))
			event.Result <- true // delete
			return
		}

		tracker, ok := p.trackers[clientID]
		if !ok {
			// no longer tracking this client
			event.Result <- true // delete
			return
		}

		event.Result <- tracker.ShouldDelete(event.Pod.Name)

	case *podwatcher.PodAdded:
		clientID, err := k8sutil.ExtractClientID(event.Pod)
		if err != nil {
			p.logger.Warn("missing client id", zap.Error(err))
			return
		}

		tracker, ok := p.trackers[clientID]
		if !ok {
			//nothing to do, no longer tracking
			return
		}

		if tracker.Executors[event.Pod.Name] {
			// already saw this pod (watcher list refresh)
			return
		}

		// first time we've seen this pod
		tracker.Executors[event.Pod.Name] = true
		tracker.NumLaunching--
		p.logger.Info("pod added",
			zap.String("name", event.Pod.Name),
			zap.Int("launching", tracker.NumLaunching))

	case *podwatcher.PodDeleted:
		clientID, err := k8sutil.ExtractClientID(event.Pod)
		if err != nil {
			p.logger.Warn("missing client id", zap.Error(err))
			return
		}

		tracker, ok := p.trackers[clientID]
		if !ok {
			//nothing to do, no longer tracking
			return
		}

		p.logger.Info("pod deleted",
			zap.String("name", event.Pod.Name))
		delete(tracker.Executors, event.Pod.Name)
	}
}

func (p *PodBackend) handleBackendEvent(event catapult.BackendEvent) {
	switch event := event.(type) {
	case *catapult.BackendCreate:
		tracker := NewTracker(event.JobSpec)
		p.trackers[event.ClientID] = tracker

	case *catapult.BackendDelete:
		// bulk delete
		p.logger.Info("delete backend", zap.String("client", event.ClientID.String()))
		go func() {
			err := k8sutil.DeleteClientPods(event.ClientID, p.namespace, p.clientset)
			p.logger.Info("bulk delete pods", zap.String("client",
				event.ClientID.String()),
				zap.Error(err))
		}()
		delete(p.trackers, event.ClientID)

	case *catapult.BackendDeleteExecutor:
		tracker, ok := p.trackers[event.ClientID]
		if !ok {
			return
		}

		p.logger.Info("delete executor",
			zap.String("client", event.ClientID.String()),
			zap.String("executor", event.ExecutorID))
		delete(tracker.Executors, event.ExecutorID)

	case *catapult.BackendResize:
		tracker, ok := p.trackers[event.ClientID]
		if !ok {
			return
		}

		logger := p.logger.With(
			zap.String("client", event.ClientID.String()),
			zap.Int("current-size", tracker.CurrentSize()),
			zap.Int("target-size", event.Size))

		switch {
		case event.Size == tracker.CurrentSize():
			// nothing to do
			logger.Info("skip resize")

		case event.Size > tracker.CurrentSize():
			// launch more

			// Limit number of concurrent launches. This works because planner
			// pulses target size periodically, so if we dont launch everything
			// now we can eventually launch more.
			numToLaunch := event.Size - tracker.CurrentSize() // number of executors to start
			maxConcurrentLaunch := 64
			if numToLaunch > maxConcurrentLaunch {
				numToLaunch = maxConcurrentLaunch
			}

			logger.Info("scale up",
				zap.Int("launching", numToLaunch))

			tracker.NumLaunching += numToLaunch

			// launch executors and eventually get notified of any errors
			go func() {
				numLaunched, err := k8sutil.BatchLaunchExecutors(
					numToLaunch,
					event.ClientID.String(),
					p.schedulerID,
					tracker.JobSpec,
					p.namespace,
					p.clientset,
				)

				p.internalEvents <- &launchFinished{
					ClientID:     event.ClientID,
					NumSubmitted: numToLaunch,
					NumLaunched:  numLaunched,
					Err:          err,
				}
			}()

		case event.Size < tracker.CurrentSize():
			// scale down
			// If we're here, planner wants to scale down but hasnt specified indidivual
			// executors to delete explicitly. For now we just ignore, relying on
			// planner to eventually explicitly specify which executors to delete
			logger.Info("unhandled case: size down without explicit executor deletes")
		}
	}
}

func (p *PodBackend) SendBackendEvent(event catapult.BackendEvent) {
	p.incomingBackendEvents <- event
}

func (p *PodBackend) SendPodEvent(event podwatcher.Event) {
	p.incomingPodEvents <- event
}
