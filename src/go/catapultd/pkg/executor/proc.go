package executor

import (
	"catapultd/pkg/gen/api"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

// TaskEnv provides task environment details
type TaskEnv struct {
	spec *api.TaskSpec
	// workDir directory to use for task specific work
	workDir string
}

// TaskCmd provides the task command ready to be run
type TaskCmd struct {
	cmd        *exec.Cmd
	stdout     *os.File
	stderr     *os.File
	stdoutPath string
	stderrPath string
	// outputDataPath gives path to on disk task data to be returned as part of the
	// final task result after the computation has finished
	outputDataPath string
}

// Exec launches task spec
func (taskEnv *TaskEnv) BuildCmd() (*TaskCmd, error) {
	inputDataPath := filepath.Join(taskEnv.workDir, "task_input")
	f, err := os.Create(inputDataPath)
	if err != nil {
		return nil, err
	}
	_, err = f.Write(taskEnv.spec.RawData)
	if err != nil {
		return nil, err
	}

	outputDataPath := filepath.Join(taskEnv.workDir, "task_result")

	cmd := exec.Command(
		"python",
		"-m", "catapult.runner",
		"--input", inputDataPath,
		"--output", outputDataPath,
	)

	// NOTE: only last values are used so no need to remove depulicates
	env := os.Environ()
	env = append(env, fmt.Sprintf("MESOS_SANDBOX=%v", taskEnv.workDir))
	env = append(env, fmt.Sprintf("CATAPULT_WORKSPACE=%v", taskEnv.workDir))
	env = append(env, "RUST_BACKTRACE=1")
	cmd.Env = env

	stdoutPath := filepath.Join(taskEnv.workDir, "stdout")
	stdout, err := os.Create(stdoutPath)
	if err != nil {
		return nil, err
	}
	cmd.Stdout = io.MultiWriter(os.Stdout, stdout)
	//cmd.Stdout = os.Stdout

	stderrPath := filepath.Join(taskEnv.workDir, "stderr")
	stderr, err := os.Create(stderrPath)
	if err != nil {
		return nil, err
	}
	cmd.Stderr = io.MultiWriter(os.Stderr, stderr)
	//cmd.Stdout = stdout

	return &TaskCmd{
		cmd:            cmd,
		stdout:         stdout,
		stderr:         stderr,
		stdoutPath:     stdoutPath,
		stderrPath:     stderrPath,
		outputDataPath: outputDataPath,
	}, nil
}

func (cmd *TaskCmd) Cleanup() {
	cmd.stdout.Close()
	cmd.stderr.Close()
}
