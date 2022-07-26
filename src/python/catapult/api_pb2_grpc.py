# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from catapult import api_pb2 as catapult_dot_api__pb2


class ExecutorServiceStub(object):
    """TaskStream handles connections from executors
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Tasks = channel.stream_stream(
            "/ExecutorService/Tasks",
            request_serializer=catapult_dot_api__pb2.ExecutorRequest.SerializeToString,
            response_deserializer=catapult_dot_api__pb2.ExecutorResponse.FromString,
        )


class ExecutorServiceServicer(object):
    """TaskStream handles connections from executors
    """

    def Tasks(self, request_iterator, context):
        """Missing associated documentation comment in .proto file"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_ExecutorServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "Tasks": grpc.stream_stream_rpc_method_handler(
            servicer.Tasks,
            request_deserializer=catapult_dot_api__pb2.ExecutorRequest.FromString,
            response_serializer=catapult_dot_api__pb2.ExecutorResponse.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "ExecutorService", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class ExecutorService(object):
    """TaskStream handles connections from executors
    """

    @staticmethod
    def Tasks(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_stream(
            request_iterator,
            target,
            "/ExecutorService/Tasks",
            catapult_dot_api__pb2.ExecutorRequest.SerializeToString,
            catapult_dot_api__pb2.ExecutorResponse.FromString,
            options,
            channel_credentials,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )


class TaskGatewayStub(object):
    """TaskGateway service handles receiving tasks from clients and sending back results
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.SubmitTasks = channel.stream_stream(
            "/TaskGateway/SubmitTasks",
            request_serializer=catapult_dot_api__pb2.JobItem.SerializeToString,
            response_deserializer=catapult_dot_api__pb2.SubmitTaskResponse.FromString,
        )


class TaskGatewayServicer(object):
    """TaskGateway service handles receiving tasks from clients and sending back results
    """

    def SubmitTasks(self, request_iterator, context):
        """SubmitTasks opens bidirectional stream for client to stream tasks
        to server for execution, and server to send back task results to client.

        The very first message should be a job spec, which details how to run
        the job.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_TaskGatewayServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "SubmitTasks": grpc.stream_stream_rpc_method_handler(
            servicer.SubmitTasks,
            request_deserializer=catapult_dot_api__pb2.JobItem.FromString,
            response_serializer=catapult_dot_api__pb2.SubmitTaskResponse.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "TaskGateway", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class TaskGateway(object):
    """TaskGateway service handles receiving tasks from clients and sending back results
    """

    @staticmethod
    def SubmitTasks(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_stream(
            request_iterator,
            target,
            "/TaskGateway/SubmitTasks",
            catapult_dot_api__pb2.JobItem.SerializeToString,
            catapult_dot_api__pb2.SubmitTaskResponse.FromString,
            options,
            channel_credentials,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
