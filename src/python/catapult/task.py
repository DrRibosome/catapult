from __future__ import annotations

from typing import Any
import uuid
import pickle

import dill

from catapult import api_pb2
from catapult import exceptions
from catapult import common

_BUCKET = "mybucket"


class TaskSpec:
    """specification of task to execute"""

    def __init__(self, name: str, func, args, kwargs):
        self.task_id = str(uuid.uuid4())
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs

        input_key = str(uuid.uuid4())
        output_key = str(uuid.uuid4())
        self.input_data = "gs://{}/catapult-input/{}".format(_BUCKET, input_key)
        self.output_data = "gs://{}/catapult-output/{}".format(_BUCKET, output_key)

    def __hash__(self):
        return hash(self.encode())

    def __eq__(self, other):
        return self.task_id == other.task_id

    def run(self) -> Any:
        """run the task

        Returns:
            Any task result
        """
        return self.func(*self.args, **self.kwargs)

    def encode(self):
        """encode spec as byte buffer"""
        return dill.dumps((self.func, self.args, self.kwargs))


class Task:  # pylint: disable=too-few-public-methods
    """internal class tracking task execution"""

    def __init__(self, spec: TaskSpec):
        self.spec = spec
        self.result = None
        self.complete = False


class TaskSet:
    """set of all tasks for a particular job
    
    Used to preserve ordering of results as tasks results come in
    out of order.
    """

    def __init__(self, specs: list[TaskSpec]):
        self.tasks = []
        self.tasks_by_id = {}
        for spec in specs:
            task = Task(spec)
            self.tasks.append(task)
            self.tasks_by_id[spec.task_id] = task

        self.num_complete = 0

    @property
    def all_complete(self):
        """returns bool indicating whether all tasks have completed"""
        return self.num_complete == len(self.tasks)

    @property
    def incomplete(self):
        """generator for incomplete tasks"""
        for task in self.tasks:
            if not task.complete:
                yield task

    def record_result(self, task_result: api_pb2.TaskResult):
        """record result from catapultd server

        Result is the object returned by the python catapult runner

        Args:
            task_result: task result from catapultd
        """
        task = self.tasks_by_id[task_result.task_id]

        if task_result.exit_code != 0 and not task_result.data:
            # task failed and no task data
            err = "task failed (no data): code={}\nstdout={}\nstderr={}".format(
                task_result.exit_code, task_result.stdout, task_result.stderr
            )
            raise exceptions.RemoteError(err, task, task_result)

        if task_result.exec_result.status == "FAIL":
            # executor failed
            err = f"exec failed: {task_result.err_msg}"
            raise exceptions.RemoteError(err, task, task_result)

        if not task_result.data:
            # task succeeded (exit code 0) but no task data??
            err_msg = (
                "task success, but no task data???\n"
                f"{task.spec.name}\n"
                f"exit code: {task_result.exit_code}\n"
                f"{task_result.stdout}\n"
                f"{task_result.stderr}"
            )
            raise exceptions.RemoteError(err_msg, task, task_result)

        # at this point task has finished (not necessarily successfully)
        # in a normal way
        value: common.Result = pickle.loads(task_result.data)
        if value.error_tb is not None:
            err_msg = "\n".join(
                [
                    "remote error:",
                    "task: " + task.spec.name,
                    value.error_tb,
                    task_result.stdout,
                    task_result.stderr,
                ]
            )
            raise exceptions.RemoteError(err_msg, task.spec, task_result)

        task.result = value.result
        task.complete = True
        self.num_complete += 1
