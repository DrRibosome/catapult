// Package exesvc handles task management and scheduling
package taskcore

import (
	"catapultd/pkg/gen/api"
	"go.uber.org/zap"
	"time"
)

type TaskManager struct {
	logger   *zap.Logger
	jobs     map[string]*Job // maps job id to jobs
	incoming chan JobAction
}

func NewTaskManager(logger *zap.Logger) *TaskManager {
	return &TaskManager{
		logger:   logger,
		jobs:     make(map[string]*Job),
		incoming: make(chan JobAction, 1024),
	}
}

func (m *TaskManager) Run() error {
	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case action := <-m.incoming:
			m.handleAction(action)

		case <-ticker.C:
			for _, job := range m.jobs {
				UpdateReplicas(job, m.logger)
			}
		}
	}
}

func (m *TaskManager) handleAction(action JobAction) {
	switch {
	case action.AddTask != nil:
		// add spec

		job, ok := m.jobs[action.JobID]
		if !ok {
			// Job does not exist, ignore
			//
			// This can happen when a client disconnects, causing the job to
			// be deleted, then executors disconnect which cause their
			// outstanding tasks to be added back into the task manager.
			m.logger.Info("ignoring add task (job does not exist)",
				zap.String("job", action.JobID),
				zap.String("task", action.AddTask.Spec.TaskId))
			return
		}

		if action.AddTask.FromExecutor {
			// got a job back from an executor that failed
			job.NumOutstanding--
		}

		m.logger.Info("add task",
			zap.String("job", action.JobID),
			zap.String("task", action.AddTask.Spec.TaskId),
			zap.Int("outstanding", job.NumOutstanding))

		//job.Tasks = append(job.Tasks, action.AddTask.Spec)
		job.Tasks <- action.AddTask.Spec
		UpdateReplicas(job, m.logger)

	case action.FinTask != nil:
		job, ok := m.jobs[action.JobID]
		if !ok {
			// task result for job that has since been removed - ignore
			return
		}
		job.NumCompleted++
		job.NumOutstanding--
		m.logger.Info("fin task",
			zap.String("job", action.JobID),
			zap.Int("total", job.NumTasks),
			zap.Int("complete", job.NumCompleted),
			zap.Int("outstanding", job.NumOutstanding))

		go func() {
			job.Results <- &api.SubmitTaskResponse{
				Response: &api.SubmitTaskResponse_Result{Result: action.FinTask.Result},
			}
		}()
		UpdateReplicas(job, m.logger)

	case action.AddJob != nil:
		m.logger.Info("add job", zap.String("job", action.JobID), zap.Int("ntasks", action.AddJob.Job.NumTasks))
		job := action.AddJob.Job
		m.jobs[job.ID] = job

	case action.DeleteJob != nil:
		m.logger.Info("del job", zap.String("job", action.JobID))
		delete(m.jobs, action.JobID)

	case action.GetJob != nil:
		m.logger.Info("get job", zap.String("job", action.JobID))
		// nil job when not exists
		job := m.jobs[action.JobID]
		action.GetJob.Result <- job
	}
}

func (m *TaskManager) Register(job *Job) {
	m.incoming <- JobAction{
		JobID:  job.ID,
		AddJob: &AddJobAction{job},
	}
}

func (m *TaskManager) Delete(jobID string) {
	m.incoming <- JobAction{
		JobID:     jobID,
		DeleteJob: &DeleteJobAction{},
	}
}

func (m *TaskManager) Submit(jobID string, fromExecutor bool, spec *api.TaskSpec) {
	m.incoming <- JobAction{
		JobID:   jobID,
		AddTask: &AddTaskAction{Spec: spec, FromExecutor: fromExecutor},
	}
}

func (m *TaskManager) FinTask(jobID string, result *api.TaskResult) {
	m.incoming <- JobAction{
		JobID:   jobID,
		FinTask: &FinTaskAction{result},
	}
}

func (m *TaskManager) GetJob(jobID string) *Job {
	result := make(chan *Job, 1)
	m.incoming <- JobAction{
		JobID: jobID,
		GetJob: &GetJobAction{
			Result: result,
		},
	}
	return <-result
}

func UpdateReplicas(job *Job, logger *zap.Logger) {
	desired := job.DesiredReplicas()
	if desired != job.ReplicaSize {
		// minimum theshold to update replicas at to prevent overloading the
		// k8s api server
		minUpdateThreshold := 5 * time.Second
		now := time.Now()
		elapsed := now.Sub(job.LastReplicaSetTime)
		if elapsed > minUpdateThreshold {
			logger.Info("updating replicas",
				zap.String("job", job.ID),
				zap.Int("from", job.ReplicaSize),
				zap.Int("to", desired),
			)

			job.BackendHandle.Resize(desired)
			job.ReplicaSize = job.DesiredReplicas()
			job.LastReplicaSetTime = now
		}
	}
}
