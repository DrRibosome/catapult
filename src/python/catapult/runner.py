from __future__ import annotations

import sys
import argparse
import traceback
import logging
import pathlib

import dill

import fetchlib

import catapult.exceptions
import catapult.common


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Runner:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path

    def read_input(self) -> bytes:
        """get input bytes"""
        logger.info("download input: %s", self.input_path)
        if self.input_path.startswith("gs://"):
            fetcher = fetchlib.Fetcher3()
            return fetcher.get(self.input_path, cache=True)

        # else, local file
        with pathlib.Path(self.input_path).open("rb") as f:
            return f.read()

    def write_output(self, buf: bytes):
        """write result to output location"""
        logger.info("write output: %s", self.output_path)
        if self.output_path.startswith("gs://"):
            fetcher = fetchlib.Fetcher3()
            fetcher.put(buf, self.output_path)
            return

        # else, local file
        with pathlib.Path(self.output_path).open("wb") as f:
            f.write(buf)

    def run_task(self) -> bytes:
        """run the function

        Returns:
            bytes: of pickled result (success or error)
        """
        try:
            logger.info("get input buf")
            buf = self.read_input()
            logger.info("unpack args")
            func, func_args, func_kwargs = dill.loads(buf)
            logger.info("run function")
            result = func(*func_args, **func_kwargs)
            logger.info("run finished!")

            result_bytes = dill.dumps(catapult.common.Result.success(result))
            if len(result_bytes) >= 4 * 1024 * 1024:
                raise catapult.exceptions.ResultTooLarge

            return result_bytes
        except:
            err_tb = traceback.format_exc()
            return dill.dumps(catapult.common.Result.error(err_tb))

    def run(self):
        """run the function and write results"""
        logger.info("starting")
        result_bytes = self.run_task()
        self.write_output(result_bytes)
        logger.info("fin")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", required=True, help="input data for the function to run"
    )
    parser.add_argument(
        "--output", required=True, help="output location to store result"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    runner = Runner(args.input, args.output)
    runner.run()
    sys.exit(0)


if __name__ == "__main__":
    main()
