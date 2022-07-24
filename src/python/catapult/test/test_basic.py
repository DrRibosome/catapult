import pytest

import catapult
import catapult.common

_SCHEDULER = "http://127.0.0.1:8080/ws"
_IMAGE = "gcr.io/myproject/risk"


def _add(a, b):
    return a + b


def test_basic():
    num_tasks = 9

    job_spec = catapult.common.JobSpec(
        name="test", parallelism=num_tasks, image=_IMAGE, scheduler=_SCHEDULER
    )

    tasks = []
    expected = []
    for i in range(num_tasks):
        name = "task: {}".format(i)
        task = catapult.TaskSpec(name, _add, (i, i * 2), {})
        tasks.append(task)
        expected.append(i + i * 2)

    try:
        results = catapult.launch(job_spec, tasks)
    except catapult.RemoteError as err:
        print(err.task)
        print(err.result)
        raise
    assert results == expected


def test_raise():
    def _raise():
        raise RuntimeError("misc")

    job_spec = catapult.common.JobSpec(
        name="test", parallelism=1, image=_IMAGE, scheduler=_SCHEDULER
    )

    task = catapult.TaskSpec("raise", _raise, (), {})
    with pytest.raises(catapult.RemoteError):
        catapult.launch(job_spec, [task])
