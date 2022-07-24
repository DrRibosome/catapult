package taskcore

import (
	"catapultd/pkg/backend"
	"catapultd/pkg/gen/api"
	"time"
)

// Job manages multiple tasks
type Job struct {
	ID             string
	MaxParallelism int
	NumTasks       int // total number of tasks
	NumCompleted   int // number of completed tasks
	NumOutstanding int // number of jobs given to executors
	Results        chan *api.SubmitTaskResponse
	Tasks          chan *api.TaskSpec

	BackendHandle      *backend.Handle
	ReplicaSize        int       // last set replica count
	LastReplicaSetTime time.Time // last time replica count was adjusted
}

// DesiredReplicas computes number of replicas we should have working on this job
func (job Job) DesiredReplicas() int {
	remaining := job.NumTasks - job.NumCompleted
	if remaining > job.MaxParallelism {
		remaining = job.MaxParallelism
	}

	return remaining
}

type JobAction struct {
	JobID string

	AddTask   *AddTaskAction
	FinTask   *FinTaskAction
	AddJob    *AddJobAction
	DeleteJob *DeleteJobAction
	GetJob    *GetJobAction
}

type AddTaskAction struct {
	Spec *api.TaskSpec
	// FromExecutor if ture, then task is being added back from an executor leaving,
	// rather than from a client upload.
	FromExecutor bool
}

type FinTaskAction struct {
	Result *api.TaskResult
}

type AddJobAction struct {
	Job *Job
}

type DeleteJobAction struct{}

type GetJobAction struct {
	Result chan *Job
}
