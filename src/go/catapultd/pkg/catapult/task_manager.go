package catapult

import (
	"catapultd/pkg/gen/api"
	"fmt"
	"go.uber.org/zap"
)

// TaskManager concurrent task tracker
type TaskManager struct {
	totalTasks     int
	completedTasks int
	pending        []*api.TaskSpec              // uploaded tasks that have not been given out to executors
	outstanding    map[ExecutorID]*api.TaskSpec // task given to an executor
	backend        Backend
	logger         *zap.Logger
}

func NewTaskManager(backend Backend, logger *zap.Logger) *TaskManager {
	return &TaskManager{
		outstanding: make(map[ExecutorID]*api.TaskSpec),
		backend:     backend,
		logger:      logger,
	}
}

// RemainingTasks gets number of remaining tasks
func (tm *TaskManager) RemainingTasks() int {
	return tm.totalTasks - tm.completedTasks
}

// AddTask adds task from client to manager
func (tm *TaskManager) AddTask(task *api.TaskSpec) {
	tm.totalTasks++
	tm.pending = append(tm.pending, task)
	tm.logger.Info("add task",
		zap.String("task", task.TaskId),
		zap.Int("pending", len(tm.pending)),
		zap.Int("outstanding", len(tm.outstanding)),
		zap.Int("complete", tm.completedTasks),
		zap.Int("total", tm.totalTasks),
	)
}

func (tm *TaskManager) FinTask(executorID ExecutorID) {
	logger := tm.logger.With(zap.String("executor", executorID))
	task, ok := tm.outstanding[executorID]
	if !ok {
		// fin task received, but executor has no tasks outstanding? This shouldnt happen
		logger.Warn("fin task, but executor has none outstanding")
		return
	}

	delete(tm.outstanding, executorID)
	tm.completedTasks++
	logger.Info("fin task",
		zap.String("executor", executorID),
		zap.String("task", task.TaskId),
		zap.Int("pending", len(tm.pending)),
		zap.Int("outstanding", len(tm.outstanding)),
		zap.Int("complete", tm.completedTasks),
		zap.Int("total", tm.totalTasks),
	)
}

// GiveTask sends task to executor for completion
//
// If no pending tasks, orders executor to shut down
func (tm *TaskManager) GiveTask(executor *Executor) {
	if _, ok := tm.outstanding[executor.ID]; ok {
		// executor already has a task, skip
		//
		// This can happen under the following events:
		// - executor A FIN task X
		// - executor B dies
		//   - RECOVER Y
		//   - GIVE Y to next unused executor, A
		// - executor A POLL arrives, resulting in double give
		return
	}

	logger := tm.logger.With(
		zap.String("client", executor.ClientID.String()),
		zap.String("executor", executor.ID),
	)

	if len(tm.pending) == 0 {
		// no tasks, shut down

		var outstandingIDs []string
		for executorID, spec := range tm.outstanding {
			outstandingIDs = append(outstandingIDs, fmt.Sprintf("%v:%v", executorID, spec.TaskId))
		}
		logger.Info("no tasks, shutting down",
			zap.String("executor", executor.ID),
			zap.Int("pending", len(tm.pending)),
			zap.Int("outstanding", len(tm.outstanding)),
			zap.Strings("outstanding-ids", outstandingIDs))

		executor.Conn.Shutdown()
		tm.backend.SendBackendEvent(&BackendDeleteExecutor{
			ClientID:   executor.ClientID,
			ExecutorID: executor.ID,
		})
		return
	}

	// pop next task
	task := tm.pending[0]
	tm.pending = tm.pending[1:]

	// mark as outstanding
	tm.outstanding[executor.ID] = task

	tm.logger.Info("give task",
		zap.String("executor", executor.ID),
		zap.String("task", task.TaskId),
		zap.Int("pending", len(tm.pending)),
		zap.Int("outstanding", len(tm.outstanding)),
		zap.Int("complete", tm.completedTasks),
		zap.Int("total", tm.totalTasks),
	)

	// send
	executor.Conn.SendTask(task)
}

// RecoverOutstanding adds an outstanding task an executor might have been working on
// back to the pool of pending tasks
//
// Returns bool indicating whether task was recovered
func (tm *TaskManager) RecoverOutstanding(executorID ExecutorID) bool {
	// get outstanding task, if any
	task, ok := tm.outstanding[executorID]

	if !ok {
		// no outstanding task
		tm.logger.Info("recover task",
			zap.String("executor", executorID),
			zap.Bool("found", ok),
		)
		return false
	}

	// clean up and add back to pending
	delete(tm.outstanding, executorID)
	tm.pending = append(tm.pending, task)

	tm.logger.Info("recover task",
		zap.String("executor", executorID),
		zap.String("task", task.TaskId),
		zap.Int("pending", len(tm.pending)),
		zap.Int("outstanding", len(tm.outstanding)),
		zap.Int("complete", tm.completedTasks),
		zap.Int("total", tm.totalTasks),
	)
	return true
}
