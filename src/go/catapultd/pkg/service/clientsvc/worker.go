package clientsvc

import (
	"catapultd/pkg/gen/api"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"sync"
	"time"
)

type Worker struct {
	outgoing chan *api.SubmitTaskResponse // msgs to be written to client
	handle   func(spec *api.TaskSpec)     // func called on each received task spec
	conn     api.TaskGateway_SubmitTasksServer
	logger   *zap.Logger

	finalErr chan error
	done     chan struct{}
	shutdown func(error)
}

func newWorker(
	handle func(spec *api.TaskSpec),
	conn api.TaskGateway_SubmitTasksServer,
	logger *zap.Logger) *Worker {

	once := new(sync.Once)
	done := make(chan struct{})
	finalErr := make(chan error, 1)
	shutdown := func(err error) {
		once.Do(func() {
			finalErr <- err
			close(done)
		})
	}

	return &Worker{
		outgoing: make(chan *api.SubmitTaskResponse, 1),
		handle:   handle,
		conn:     conn,
		logger:   logger,

		done:     done,
		finalErr: finalErr,
		shutdown: shutdown,
	}
}

func (w *Worker) Run() error {
	// If either read or write loop exits, exit run. Then client service
	// will exit the handler function, causing the connection to close,
	// and the remaining loop to exit.
	go func() {
		err := w.readLoop()

		// Will receive an io.EOF error when client has finished writing
		// all results to the server. Dont start the shutdown process
		// in this case
		if err != io.EOF {
			w.shutdown(err)
		}

		w.logger.Info("read loop shutdown", zap.Error(err))
	}()

	go func() {
		err := w.writeLoop()
		w.shutdown(err)
		w.logger.Info("write loop shutdown", zap.Error(err))
	}()

	go func() {
		err := w.heartbeatLoop()
		w.shutdown(err)
		w.logger.Info("heartbeat loop shutdown", zap.Error(err))
	}()

	return <-w.finalErr
}

func (w *Worker) readLoop() error {
	for {
		item, err := w.conn.Recv()
		if err != nil {
			return err
		}

		// parse out task spec
		var taskSpec *api.TaskSpec
		switch item := item.Item.(type) {
		case *api.JobItem_TaskSpec:
			taskSpec = item.TaskSpec
		default:
			return errors.New("recv non task spec msg")
		}

		// forward task spec to task manager
		w.handle(taskSpec)
	}
}

func (w *Worker) writeLoop() error {
	for {
		select {
		case resp := <-w.outgoing:
			err := w.conn.Send(resp)
			if err != nil {
				return err
			}

		case <-w.conn.Context().Done():
			// shut down
			return nil
		}
	}
}

func (w *Worker) heartbeatLoop() error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			resp := &api.SubmitTaskResponse{
				Response: &api.SubmitTaskResponse_Heartbeat{
					Heartbeat: &api.Heartbeat{},
				},
			}
			w.send(resp)

		case <-w.done:
			return nil
		}
	}
}

// send task to write loop
func (w *Worker) send(resp *api.SubmitTaskResponse) {
	// select required so that we dont block indefinitely when write loop shut down
	select {
	case <-w.done:
		// shutting down, abort
		return

	case w.outgoing <- resp:
		// sent
	}
}

func (w *Worker) SendMsg(msg string) {
	resp := &api.SubmitTaskResponse{
		Response: &api.SubmitTaskResponse_Msg{
			Msg: &api.ServerMessage{Msg: msg},
		},
	}

	w.send(resp)
}

func (w *Worker) SendResult(result *api.TaskResult) {
	resp := &api.SubmitTaskResponse{
		Response: &api.SubmitTaskResponse_Result{
			Result: result,
		},
	}

	w.send(resp)
}

func (w *Worker) Shutdown() {
	w.shutdown(nil)
}
