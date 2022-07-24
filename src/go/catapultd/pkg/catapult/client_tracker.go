package catapult

import (
	"go.uber.org/zap"
)

// ClientTracker handles tracking everything associated with a single client
type ClientTracker struct {
	Client    *Client
	Tasks     *TaskManager
	Executors map[ExecutorID]*Executor // active executors for this client
	logger    *zap.Logger
}

func NewClientTracker(client *Client, backend Backend, logger *zap.Logger) *ClientTracker {
	return &ClientTracker{
		Client:    client,
		Tasks:     NewTaskManager(backend, logger),
		Executors: make(map[ExecutorID]*Executor),
		logger:    logger,
	}
}

func (tracker *ClientTracker) UnusedExecutors() (unused []*Executor) {
	for _, exe := range tracker.Executors {
		_, ok := tracker.Tasks.outstanding[exe.ID]
		if !ok {
			unused = append(unused, exe)
		}
	}
	return
}

// RemoveExecutor cleans up executor and removes from tracker
func (tracker *ClientTracker) RemoveExecutor(executorID ExecutorID) {
	delete(tracker.Executors, executorID)

	recovered := tracker.Tasks.RecoverOutstanding(executorID)
	if !recovered {
		// no outstanding task
		return
	}

	// else executor had task, attempt to give to unused executor
	unused := tracker.UnusedExecutors()
	if len(unused) == 0 {
		// no executors
		return
	}

	first := unused[0]
	tracker.Tasks.GiveTask(first)
}
