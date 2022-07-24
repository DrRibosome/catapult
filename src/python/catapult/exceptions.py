from . import api_pb2


class RemoteError(Exception):
    """exception that occured during remote execution"""

    def __init__(self, msg: str, task, result: api_pb2.TaskResult):
        """create remote error

        Args:
            msg: error message
            task (TaskSpec): task that caused the error
        """
        super().__init__(msg)
        self.task = task
        self.result = result


class ResultTooLarge(Exception):
    """task finished successfully, but result too large to return"""
