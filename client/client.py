import grpc
import raft_pb2, raft_pb2_grpc
import time

class RaftClient:
    def __init__(self, node_addresses):
        self.node_addresses = node_addresses
        self.leader_id = 4
        
    def get_leader(self):
        # Function does not work right now. Needs to be changed.
        for node_address in self.node_addresses:
            try:
                print(node_address)
                with grpc.insecure_channel(node_address) as channel:
                    stub = raft_pb2_grpc.RaftNodeStub(channel)
                    response = stub.ServeClient(raft_pb2.ServeClientRequest(request = 'GET LEADER'))
                    if response.leader_id:
                        return response.leader_id
                    else:
                        return int(node_address.split(":")[1]) - 50050
            except:
                continue
        return None
        

    def get(self, key):
        leader_ip = f'localhost:{50050 + self.leader_id}'
        with grpc.insecure_channel(leader_ip) as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            response = stub.ServeClient(raft_pb2.ServeClientRequest(request = f'GET {key}'))
            try:
                if response.leader_id:
                    if response.leader_id == "NONE":
                        time.sleep(0.1)
                        return self.get(key)
                    self.leader_id = response.leader_id
                    return self.get(key)
            except AttributeError:
                if response.data:
                    return response.data
                else:
                    return "Key not found"

    def set(self, key, value):
        leader_ip = f'localhost:{50050 + self.leader_id}'
        with grpc.insecure_channel(leader_ip) as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            response = stub.ServeClient(raft_pb2.ServeClientRequest(request = f'SET {key} {value}'))
            try:
                if response.leader_id:
                    if response.leader_id == "NONE":
                        time.sleep(0.1)
                        return self.set(key, value)
                    self.leader_id = response.leader_id
                    return self.set(key, value)
            except AttributeError:
                if response.success:
                    return "OK"