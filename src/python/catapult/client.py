from typing import List, Generator

import tqdm
import grpc

from catapult.task import TaskSpec, TaskSet
from catapult import api_pb2, JobSpec
from catapult import api_pb2_grpc


def _spec_generator(
    job_spec: JobSpec, specs: List[TaskSpec]
) -> Generator[api_pb2.JobItem]:
    # send job spec as first message
    job = api_pb2.JobSpec(
        name=job_spec.name,
        num_tasks=len(specs),
        parallelism=job_spec.parallelism,
        image=job_spec.image,
    )
    yield api_pb2.JobItem(job_spec=job)

    for spec in specs:
        buf = spec.encode()
        outgoing = api_pb2.TaskSpec(
            task_id=spec.task_id, command="python", raw_data=buf
        )
        yield outgoing


class Client:
    def __init__(self, job_spec: JobSpec, specs: List[TaskSpec]) -> None:
        self.job_spec = job_spec
        self.task_set = TaskSet(specs)

    def task_generator(self):
        # send job spec as first message
        job = api_pb2.JobSpec(
            name=self.job_spec.name,
            num_tasks=len(self.task_set.tasks),
            parallelism=self.job_spec.parallelism,
            image=self.job_spec.image,
        )
        yield api_pb2.JobItem(job_spec=job)

        for spec in self.task_set.incomplete:
            buf = spec.encode()
            outgoing = api_pb2.TaskSpec(
                task_id=spec.task_id, command="python", raw_data=buf
            )
            yield outgoing

    def run(self, *, progress=False):
        with tqdm.tqdm(
            total=len(self.task_set.tasks), ascii=True, disable=not progress
        ) as pbar:
            while not self.task_set.all_complete:
                try:
                    self._run(pbar)
                except grpc.RpcError as err:
                    print(f"code: {err.code()}: {err}")
                    pass

    def _run(self, pbar: tqdm.tqdm):
        with grpc.insecure_channel("127.0.0.1:8081") as channel:
            stub = api_pb2_grpc.TaskGatewayStub(channel)
            for resp in stub.SubmitTasks(self.task_generator()):
                typ = resp.WhichOneof("response")

                if typ == "result":
                    task_result = resp.result

                    self.task_set.record_result(task_result)
                    pbar.update()

                    if self.task_set.all_complete:
                        return
                else:
                    raise ValueError(f"unhandled response type: {typ}")
