// Package clientsvc handles server interaction with clients
package clientsvc

import (
	"catapultd/pkg/catapult"
	"catapultd/pkg/gen/api"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// ClientService service for clients to connect to
type ClientService struct {
	planner *catapult.Planner
	logger  *zap.Logger
}

func NewClientService(planner *catapult.Planner, logger *zap.Logger) *ClientService {
	return &ClientService{
		planner: planner,
		logger:  logger,
	}
}

func (cs *ClientService) SubmitTasks(conn api.TaskGateway_SubmitTasksServer) error {
	cs.logger.Info("new client connected")

	// receive job spec as first message
	item, err := conn.Recv()
	if err != nil {
		cs.logger.Warn("failed to recv job spec", zap.Error(err))
		return err
	}

	// parse first message as job spec
	var jobSpec *api.JobSpec
	switch item := item.Item.(type) {
	case *api.JobItem_JobSpec:
		jobSpec = item.JobSpec
	default:
		return errors.New("expected first message to be job spec")
	}

	// create new client
	clientID := uuid.New()

	handleTask := func(task *api.TaskSpec) {
		cs.planner.Send(&catapult.ClientAddTask{
			ClientID: clientID,
			Task:     task,
		})
	}

	worker := newWorker(handleTask, conn, cs.logger)

	client := catapult.NewClient(clientID, worker, jobSpec)
	cs.planner.Send(&catapult.ClientConnect{
		Client: client,
	})

	// start worker to handle connection
	workerErr := worker.Run()
	cs.logger.Info("worker finished", zap.Error(workerErr))

	cs.planner.Send(&catapult.ClientDisconnect{
		Client: client,
	})

	return workerErr
}
