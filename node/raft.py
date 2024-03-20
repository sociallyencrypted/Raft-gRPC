import grpc
import threading
from protos import raft_pb2, raft_pb2_grpc
from storage import Storage

class RaftNode(raft_pb2_grpc.RaftNodeServicer):
    def __init__(self, node_id, node_addresses):
        self.node_id = node_id
        self.node_addresses = node_addresses
        self.storage = Storage(node_id)
        

    def AppendEntries(self, request, context):
        # Handle AppendEntries RPC
        # Process incoming entries, update log, and respond accordingly
        pass

    def RequestVote(self, request, context):
        # Handle RequestVote RPC
        pass

    def ServeClient(self, request, context):
        if request.action == raft_pb2.GET:
            if self.role == "leader":
                value = self.storage.get(request.key) # Get the value from the storage
                return raft_pb2.ClientResponse(value=value) # Return the value to the client
            else:
                if self.leader_id:
                    return raft_pb2.ClientResponse(leader_id=self.leader_id) # Return the leader_id to the client
                else:
                    return raft_pb2.ClientResponse(leader_id="NONE") # Return "NONE" if there is no leader
        elif request.action == raft_pb2.SET:
            if self.role == "leader":
                self.storage.append_log(request.entry) # Append the log to the storage
                # propogate the log to other nodes
                for node_address in self.node_addresses:
                    if node_address != f'node:{50050 + self.node_id}': # Skip the current node
                        threading.Thread(target=self.replicate_log, args=(node_address, request.entry)).start() # Start a new thread to replicate the log
                return raft_pb2.ClientResponse(value="OK") # Return "OK" to the client
        else:
            if self.leader_id:
                return raft_pb2.ClientResponse(leader_id=self.leader_id) # Return the leader_id to the client
            else:
                return raft_pb2.ClientResponse(leader_id="NONE") # Return "NONE" if there is no leader
            
    def replicate_log(self, node_address, entry):
        # Function to be completed
        pass

    # Other helper methods for Raft state machine, leader election, log replication, etc.