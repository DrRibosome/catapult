package taskcore

type Command struct {
	JobID  string
	action trackerAction

	AddTask   *AddTaskAction
	FinTask   *FinTaskAction
	AddJob    *AddJobAction
	DeleteJob *DeleteJobAction
	GetJob    *GetJobAction
}
