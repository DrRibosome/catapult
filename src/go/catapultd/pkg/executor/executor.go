// Package executor deals with executing client tasks
package executor

import (
	"catapultd/pkg/gen/api"
	"cloud.google.com/go/storage"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Config executor config
type Config struct {
	ID      string // executor id
	WorkDir string // working directory
	JobID   string
}

type Executor struct {
	incoming      chan *api.ExecutorResponse
	outgoing      chan *api.ExecutorRequest
	config        Config
	storageClient *storage.Client
	conn          api.ExecutorService_TasksClient
	logger        *zap.Logger

	finalErr chan error
	shutdown func(error)
}

func New(config Config, storageClient *storage.Client, conn api.ExecutorService_TasksClient, logger *zap.Logger) *Executor {
	//client := api.NewExecutorServiceClient(conn)

	finalErr := make(chan error, 1)
	once := new(sync.Once)
	shutdown := func(err error) {
		once.Do(func() {
			finalErr <- err
		})
	}

	return &Executor{
		incoming:      make(chan *api.ExecutorResponse),
		outgoing:      make(chan *api.ExecutorRequest),
		config:        config,
		storageClient: storageClient,
		conn:          conn, // stream, err := exe.client.Tasks(context.Background())
		logger:        logger,

		finalErr: finalErr,
		shutdown: shutdown,
	}
}

func (exe *Executor) Run() error {
	exe.logger.Info("starting",
		zap.String("job", exe.config.JobID),
		zap.String("id", exe.config.ID))

	go func() {
		err := exe.readLoop()
		exe.logger.Info("read loop shut down", zap.Error(err))
		exe.shutdown(err)
	}()

	go func() {
		err := exe.writeLoop()
		exe.logger.Info("write loop shut down", zap.Error(err))
		exe.shutdown(err)
	}()

	go func() {
		err := exe.heartbeatLoop()
		exe.logger.Info("heartbeat loop shut down", zap.Error(err))
		exe.shutdown(err)
	}()

	go func() {
		err := exe.taskLoop()
		exe.logger.Info("task loop shut down", zap.Error(err))
		exe.shutdown(err)
	}()

	return <-exe.finalErr
}

func (exe *Executor) Shutdown() {
	exe.shutdown(nil)
}

func (exe *Executor) readLoop() error {
	for {
		resp, err := exe.conn.Recv()
		if err != nil {
			return err
		}

		switch resp.Response.(type) {
		case *api.ExecutorResponse_Heartbeat:
			// filter out heartbeats
			continue

		case *api.ExecutorResponse_Shutdown:
			return nil
		}

		exe.incoming <- resp
	}
}

// sendRequest queue request to be written by write loop
func (exe *Executor) sendRequest(req *api.ExecutorRequest) {
	select {
	case exe.outgoing <- req:
	case <-exe.conn.Context().Done():
		// shutting down - ignore
	}
}

func (exe *Executor) writeLoop() error {
	for {
		select {
		case req := <-exe.outgoing:
			err := exe.conn.Send(req)
			if err != nil {
				return err
			}

		case <-exe.conn.Context().Done():
			return nil
		}
	}
}

func (exe *Executor) heartbeatLoop() error {
	ticker := time.NewTimer(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			req := &api.ExecutorRequest{
				Action: &api.ExecutorRequest_Heartbeat{
					Heartbeat: &api.Heartbeat{},
				},
			}
			exe.sendRequest(req)

		case <-exe.conn.Context().Done():
			return nil
		}
	}
}

// taskLoop handles running tasks and requesting new tasks
func (exe *Executor) taskLoop() error {
	// send initial message
	exe.sendRequest(&api.ExecutorRequest{
		Action: &api.ExecutorRequest_ExecutorInit{
			ExecutorInit: &api.ExecutorInit{
				Id:       exe.config.ID,
				ClientId: exe.config.JobID,
			},
		},
	})

	for {
		exe.logger.Info("requesting task")
		request := &api.ExecutorRequest{
			Action: &api.ExecutorRequest_TaskRequest{
				TaskRequest: &api.TaskRequest{
					JobId: exe.config.JobID,
				},
			},
		}
		exe.sendRequest(request)

		exe.logger.Info("awaiting task")

		select {
		case resp := <-exe.incoming:
			err := exe.handleResponse(resp)
			if err != nil {
				return err
			}

		case <-exe.conn.Context().Done():
			return nil
		}
	}

	return nil
}

func (exe *Executor) handleResponse(resp *api.ExecutorResponse) error {
	exe.logger.Info("start task")

	var spec *api.TaskSpec
	switch msg := resp.Response.(type) {
	case *api.ExecutorResponse_TaskSpec:
		spec = msg.TaskSpec
	default:
		return errors.Errorf("unexpected msg: %v", msg)
	}

	// run task
	//
	// Restart on non-zero exit code, which is unxepected and indicates a
	// runner failure. Most common reason is fetcherd not available (for
	// instance, pod container started before fetcherd).
	maxAttempts := 8
	var result *api.TaskResult
	for i := 0; i < maxAttempts; i++ {
		exe.logger.Info("running task", zap.String("task", spec.TaskId))
		var err error
		result, err = exe.runTask(spec)
		if err != nil {
			return err
		}

		if result.ExitCode == 0 {
			break
		}

		if i != maxAttempts-1 {
			// dont sleep on last run
			time.Sleep(time.Duration(500*i) * time.Millisecond)
		}
	}

	// send task result
	exe.logger.Info("sending task result", zap.String("task", spec.TaskId))
	exe.logger.Sugar().Infof("result: %v", result)
	action := &api.ExecutorRequest{
		Action: &api.ExecutorRequest_TaskResult{
			TaskResult: result,
		},
	}

	exe.sendRequest(action)
	return nil
}

// runTask runs task to completion, return result or err for why run failed
//
// As a note, if the underlying task starts successfully, but exits with a non-zero
// exit code, this func will *not* return an error. Only failure to launch the task
// counts as an error.
func (exe *Executor) runTask(spec *api.TaskSpec) (*api.TaskResult, error) {
	workDir := filepath.Join(exe.config.WorkDir, spec.TaskId)
	exe.logger.Info("setting up task", zap.String("workdir", workDir))
	if err := os.MkdirAll(workDir, os.ModePerm); err != nil {
		return nil, err
	}

	taskEnv := TaskEnv{
		spec:    spec,
		workDir: workDir,
	}

	taskCmd, err := taskEnv.BuildCmd()
	if err != nil {
		// error building task command
		exe.logger.Warn("failed to build command", zap.Error(err))
		return nil, err
	}

	exe.logger.Info("command",
		zap.Strings("args", taskCmd.cmd.Args),
	)

	exe.logger.Info("launching task", zap.String("workdir", workDir))
	exitCode, err := exe.launch(taskCmd.cmd)
	exe.logger.Info("task fin", zap.Int("exitcode", exitCode), zap.Error(err))
	taskCmd.stdout.Close()
	taskCmd.stderr.Close()

	base := uuid.New().String()
	stdoutKey := fmt.Sprintf("%v/stdout", base)
	stderrKey := fmt.Sprintf("%v/stderr", base)

	stdoutGSPath := fmt.Sprintf("gs://%v/%v", logBucket, stdoutKey)
	stderrGSPath := fmt.Sprintf("gs://%v/%v", logBucket, stderrKey)

	// upload stdout,stderr
	uploadWaits := new(sync.WaitGroup)
	uploadWaits.Add(2)
	go func() {
		defer uploadWaits.Done()
		exe.logger.Info("upload stdout",
			zap.String("from", taskCmd.stdoutPath),
			zap.String("to", stdoutGSPath),
		)
		if err := uploadFile(taskCmd.stdoutPath, stdoutKey, exe.storageClient); err != nil {
			exe.logger.Warn("failed to upload stdout", zap.Error(err))
		}
	}()
	go func() {
		defer uploadWaits.Done()
		exe.logger.Info("upload stderr",
			zap.String("from", taskCmd.stderrPath),
			zap.String("to", stderrGSPath),
		)
		if err := uploadFile(taskCmd.stderrPath, stderrKey, exe.storageClient); err != nil {
			exe.logger.Warn("failed to upload stderr", zap.Error(err))
		}
	}()

	// wait for std out/err to upload, then remove work dir
	uploadWaits.Wait()
	defer os.RemoveAll(workDir)

	execResult := api.ExecResult{
		Status: api.ExecResult_SUCCESS,
	}

	if err != nil {
		execResult = api.ExecResult{
			Status: api.ExecResult_FAIL,
			ErrMsg: err.Error(),
		}
	}

	// read task data from disk
	taskData, err := readTaskResultData(taskCmd.outputDataPath)
	if err != nil && execResult.Status == api.ExecResult_SUCCESS {
		// failed to read task data (and exec result otherwise successful)
		execResult = api.ExecResult{
			Status: api.ExecResult_FAIL,
			ErrMsg: err.Error(),
		}
	}

	result := &api.TaskResult{
		JobId:      exe.config.JobID,
		TaskId:     spec.TaskId,
		ExitCode:   int32(exitCode),
		Stdout:     stdoutGSPath,
		Stderr:     fmt.Sprintf("gs://%v/%v", logBucket, stderrKey),
		Data:       taskData,
		ExecResult: &execResult,
	}
	return result, nil
}

// readTaskResultData reads task result data file from disk
//
// Returns error if task data is too large for grpc (4mb). If task data is
// missing, return empty data instead.
func readTaskResultData(path string) (empty []byte, loadErr error) {
	info, err := os.Stat(path)
	if err != nil {
		loadErr = err
		return
	}

	const maxSize int64 = 4 * 1024 * 1024
	if info.Size() > maxSize {
		loadErr = errors.Errorf("task data too large: found %v > max %v", info.Size(), maxSize)
		return
	}

	file, err := os.Open(path)
	if err != nil {
		loadErr = errors.Wrap(err, "unexpected err reading task data")
		return
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		loadErr = errors.Wrap(err, "unexpected err reading task data")
		return
	}

	return data, nil
}

func (exe *Executor) launch(cmd *exec.Cmd) (exitCode int, err error) {
	// indicate that cmd should get its own process group id (instead of inheriting)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	exe.logger.Info("start process")
	err = cmd.Start()
	if err != nil {
		return
	}

	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err == nil {
		// Kill process group afterwards
		defer syscall.Kill(-pgid, syscall.SIGKILL)
	}

	exe.logger.Info("wait process")
	if err = cmd.Wait(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			// exited with non-zero exit code
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				exitCode = status.ExitStatus()

				// Python assigns negative numbers to signals it receives.
				// Not sure why, but occasionally python will exit with SIGHUP
				// (eg, exit -1), which doesnt appear to be an error according
				// to the logs. To prevent a restart, we correct the exit code
				if exitCode == -1 {
					exe.logger.Sugar().Info("correcting exit code: -1 -> 0")
					exitCode = 0
				}

				err = nil
				return
			}
		}
	}

	return
}
