import grpc
import raft_pb2, raft_pb2_grpc
import time

class RaftClient:
    def __init__(self, node_addresses):
        self.node_addresses = node_addresses
        self.leader_id = self.get_leader()
        
    def get_leader(self):
        for node_address in self.node_addresses:
            try:
                print(node_address)
                with grpc.insecure_channel(node_address) as channel:
                    stub = raft_pb2_grpc.RaftNodeStub(channel)
                    response = stub.ServeClient(raft_pb2.ServeClientRequest(request="GET LEADER"))
                    if response.leader_id:
                        return response.leader_id
                    else:
                        return int(node_address.split(":")[1]) - 50050
            except:
                continue
        return None
        

    def get(self, key):
        leader_ip = self.node_addresses[self.leader_id]
        with grpc.insecure_channel(leader_ip) as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            response = stub.ServeClient(raft_pb2.ServeClientRequest(request = f'GET {key}'))
            if response.leader_id: # This means the node to which the request was sent is not a leader. It hence sends a guess of who the leader is.
                if response.leader_id == "NONE": # This means there is no leader. The client should try again later.
                    time.sleep(0.1)
                    return self.get(key)
                self.leader_id = response.leader_id # Update the leader_id to the one guessed by the node,
                return self.get(key) # and try again.
            else:
                if response.value:
                    return response.value
                else:
                    return "Key not found"

    def set(self, key, value):
        leader_ip = self.node_addresses[self.leader_id]
        with grpc.insecure_channel(leader_ip) as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            response = stub.ServeClient(raft_pb2.ServeClientRequest(request = f'SET {key} {value}'))
            if response.leader_id:
                if response.leader_id == "NONE":
                    time.sleep(0.1)
                    return self.set(key, value)
                self.leader_id = response.leader_id
                return self.set(key, value)
            else:
                if response.value == "OK":
                    return "OK"