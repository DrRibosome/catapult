// Package exesvc handles server interaction with executors
package exesvc

import (
	"catapultd/pkg/catapult"
	"catapultd/pkg/gen/api"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// ExecutorService provides service for executors to interact with
type ExecutorService struct {
	planner *catapult.Planner
	logger  *zap.Logger
}

func NewExecutorService(planner *catapult.Planner, logger *zap.Logger) *ExecutorService {
	return &ExecutorService{
		planner: planner,
		logger:  logger,
	}
}

func (s *ExecutorService) Run() error {
	return nil
}

func (s *ExecutorService) Tasks(conn api.ExecutorService_TasksServer) error {
	// NOTE: *returning* from this method closes server side of the
	// biderectional stream, which is why there's no close method
	//
	// See: https://github.com/grpc/grpc-go/issues/444
	s.logger.Info("new task stream")

	// receive initial exe init message
	first, err := conn.Recv()
	if err != nil {
		return err
	}

	var id catapult.ExecutorID
	var clientID catapult.ClientID
	switch msg := first.Action.(type) {
	case *api.ExecutorRequest_ExecutorInit:
		id = catapult.ExecutorID(msg.ExecutorInit.Id)
		clientID, err = uuid.Parse(msg.ExecutorInit.ClientId)
		if err != nil {
			return errors.Wrapf(err, "failed to parse client id: %v", msg.ExecutorInit.ClientId)
		}

	default:
		return errors.Errorf("unexpected non-init first message: %v", first)
	}

	handle := func(req *api.ExecutorRequest) error {
		switch msg := req.Action.(type) {
		case *api.ExecutorRequest_TaskRequest:
			s.planner.Send(&catapult.ExecutorPollTask{
				ClientID:   clientID,
				ExecutorID: id,
			})

		case *api.ExecutorRequest_TaskResult:
			s.planner.Send(&catapult.ExecutorFinTask{
				ClientID:   clientID,
				ExecutorID: id,
				Result:     msg.TaskResult,
			})

		default:
			return errors.Errorf("unexpected non-init first message: %v", first)
		}

		return nil
	}

	worker := newWorker(handle, conn, s.logger)

	executor := catapult.NewExecutor(id, clientID, worker)

	s.planner.Send(&catapult.ExecutorConnect{
		Executor: executor,
	})

	err = worker.Run()
	s.logger.Info("exiting worker handler", zap.Error(err))

	s.planner.Send(&catapult.ExecutorDisconnect{
		Executor: executor,
	})

	return err
}
