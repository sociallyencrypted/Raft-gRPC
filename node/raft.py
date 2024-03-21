import grpc
import raft_pb2, raft_pb2_grpc
from node.storage import Storage
from concurrent import futures

class RaftNode(raft_pb2_grpc.RaftNodeServicer):
    def __init__(self, node_id, node_addresses):
        self.node_id = node_id
        self.node_addresses = node_addresses
        self.storage = Storage(node_id)
        self.role = "leader" # Hardcoded for now. Needs to be changed
        self.leaderId = node_id # Hardcoded for now. Needs to be changed
        
    def serve(node):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftNodeServicer_to_server(node, server)
        server.add_insecure_port(f'localhost:{50050 + node.node_id}')
        server.start()
        print(f"Server started on port {50050 + node.node_id}")
        server.wait_for_termination()     

    def AppendEntries(self, request, context):
        # Handle AppendEntries RPC
        # Process incoming entries, update log, and respond accordingly
        pass

    def RequestVote(self, request, context):
        # Handle RequestVote RPC
        pass

    def ServeClient(self, request, context):
        request = request.request.split(" ")
        if request[0] == "GET":
            if self.role == "leader":
                key = request[1] # Get the key from the request
                if key == "LEADER":
                    return raft_pb2.ServeClientReply(leaderId=self.node_id)
                value = self.storage.get(key)
                return raft_pb2.ServeClientReply(data=value) # Return the value to the client
            else:
                if self.leaderId:
                    return raft_pb2.ServeClientReply(leaderId=self.leaderId) # Return the leaderId to the client
                else:
                    return raft_pb2.ServeClientReply(leaderId="NONE") # Return "NONE" if there is no leader
        elif request[0] == "SET":
            key = request[1] # Get the key from the request
            value = request[2] # Get the value from the request
            if self.role == "leader":
                self.storage.append_log(key, value)
                for node in self.node_addresses:
                    if node != f'localhost:{50050 + self.node_id}':
                        self.replicate_log(node, key, value)
                return raft_pb2.ServeClientReply(success=True) # Return "OK" to the client
        else:
            if self.leaderId:
                return raft_pb2.ServeClientReply(leaderId=self.leaderId) # Return the leaderId to the client
            else:
                return raft_pb2.ServeClientReply(leaderId="NONE") # Return "NONE" if there is no leader
            
    def replicate_log(self, node_address, key, value):
        # Use the AppendEntries RPC to replicate log entries to other nodes
        # To be implemented
        pass

    # Other helper methods for Raft state machine, leader election, log replication, etc.