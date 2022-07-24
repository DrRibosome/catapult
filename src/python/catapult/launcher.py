# pylint: disable=bad-continuation

from typing import Dict, List
import time
import concurrent.futures
import dataclasses

import tqdm
import grpc

import fetchlib

from catapult.task import TaskSpec, TaskSet
from catapult import api_pb2, Resources, JobSpec
from catapult import api_pb2_grpc
import catapult.common


def _spec_generator(job_spec: JobSpec, specs: List[TaskSpec]):
    """generator for api JobItem objects

    Handles uploading spec data to GCS, then yielding specs when done.
    Also handles sending first JobSpec message to indicate job params.

    Args:
        job_spec: job spec, as from `job_spec` func
        specs: task specs to run

    Returns generator of api_pb2.JobItem
    """
    # send job spec as first message
    job = api_pb2.JobSpec(
        name=job_spec.name,
        num_tasks=len(specs),
        parallelism=job_spec.parallelism,
        image=job_spec.image,
        resources=api_pb2.Resources(
            cpus=job_spec.resources.cpus, mem=job_spec.resources.mem
        ),
    )
    yield api_pb2.JobItem(job_spec=job)

    for spec in specs:
        buf = spec.encode()
        outgoing_spec = api_pb2.TaskSpec(
            task_id=spec.task_id, command="python", raw_data=buf
        )

        yield api_pb2.JobItem(task_spec=outgoing_spec)


def launch(job_spec: JobSpec, specs: List[TaskSpec], *, progress=False) -> List:
    if len(specs) == 0:
        return []
    task_set = TaskSet(specs)
    while True:
        remaining = [task.spec for task in task_set.incomplete]
        try:
            _launch(job_spec, remaining, task_set, progress=progress)
        except grpc.RpcError as err:
            print(err)

        if task_set.all_complete:
            break

        # Else, still incomplete tasks remaining. Wait, then try again
        time.sleep(1)

    # collect and return results
    results = []
    for task in task_set.tasks:
        results.append(task.result)

    return results


def _launch(
    job_spec: JobSpec, specs: List[TaskSpec], task_set: TaskSet, *, progress=False
) -> List:
    host = catapult.common.get_scheduler_host()

    iter_err = None

    def exception_wrapper():
        """janky exception wrapper to return errors encountered while generating"""
        try:
            for v in _spec_generator(job_spec, specs):
                yield v
        except Exception as err:
            nonlocal iter_err
            iter_err = err
            print(err)
            raise

    try:
        with grpc.insecure_channel(host) as channel:
            stub = api_pb2_grpc.TaskGatewayStub(channel)
            count = 0
            with tqdm.tqdm(total=len(specs), ascii=True, disable=not progress) as pbar:
                for resp in stub.SubmitTasks(exception_wrapper()):
                    typ = resp.WhichOneof("response")

                    if typ == "result":
                        # task result
                        task_result = resp.result

                        task_set.record_result(task_result)
                        pbar.update(1)
                        count += 1

                        if count == len(specs):
                            # finished receiving tasks
                            break

                    elif typ == "msg":
                        # server message, display
                        print(resp.msg.msg)

                    elif typ == "heartbeat":
                        # heartbeat, ignore
                        pass

                    else:
                        raise ValueError(f"unhandled response type: {typ}")

    except grpc.RpcError:
        if iter_err is not None:
            raise RuntimeError("error iterating tasks") from iter_err

        # else, some other error
        raise
