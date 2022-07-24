import argparse

import catapult
import catapult.common

_SCHEDULER = "127.0.0.1:8081"
_RESOURCES = {"cpus": 0.1, "mem": 256, "disk": 32}


def _add(a, b):
    print("here")
    return a + b


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=99)
    parser.add_argument("--parallelism", type=int, default=16)
    parser.add_argument("--image", type=str, required=True)
    args = parser.parse_args()

    num_tasks = args.tasks

    job_spec = catapult.common.JobSpec(
        name="test",
        parallelism=args.parallelism,
        image=args.image,
        scheduler=_SCHEDULER,
    )

    tasks = []
    expected = []
    for i in range(num_tasks):
        name = "task: {}".format(i)
        task = catapult.TaskSpec(name, _add, (i, i * 2), {})
        tasks.append(task)
        expected.append(i + i * 2)

    # print(list(catapult._spec_generator(job_spec, tasks)))

    results = catapult.launch(job_spec, tasks, progress=True)
    assert results == expected


if __name__ == "__main__":
    main()
