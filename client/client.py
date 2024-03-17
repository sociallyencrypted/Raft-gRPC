import grpc
from protos import raft_pb2, raft_pb2_grpc

class RaftClient:
    def __init__(self, node_addresses):
        self.node_addresses = node_addresses
        self.leader_id = None
        # Establish gRPC channel with one of the nodes

    def get(self, key):
        # Send GET request to leader
        # Handle leader redirection and retry
        pass

    def set(self, key, value):
        # Send SET request to leader
        # Handle leader redirection and retry
        pass