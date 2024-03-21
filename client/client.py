import grpc
import raft_pb2, raft_pb2_grpc
import time

class RaftClient:
    def __init__(self, node_addresses):
        self.node_addresses = node_addresses
        self.leaderId = self.get_leader()
        
    def get_leader(self):
        for node_address in self.node_addresses:
            try:
                with grpc.insecure_channel(node_address) as channel:
                    stub = raft_pb2_grpc.RaftNodeStub(channel)
                    response = stub.ServeClient(raft_pb2.ServeClientRequest(request = 'GET LEADER'))
                    if response.leaderId:
                        print("Leader found at port", int(response.leaderId) + 50050)
                        return response.leaderId
                    else:
                        print("Leader found at port", int(node_address.split(":")[1]) - 50050)
                        return int(node_address.split(":")[1]) - 50050
            except Exception as e:
                continue
        return None
        

    def get(self, key):
        leader_ip = f'localhost:{50050 + self.leaderId}'
        with grpc.insecure_channel(leader_ip) as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            response = stub.ServeClient(raft_pb2.ServeClientRequest(request = f'GET {key}'))
            try:
                if response.leaderId:
                    if response.leaderId == "NONE":
                        time.sleep(0.1)
                        return self.get(key)
                    self.leaderId = response.leaderId
                    return self.get(key)
            except AttributeError:
                if response.data:
                    return response.data
                else:
                    return "Key not found"

    def set(self, key, value):
        leader_ip = f'localhost:{50050 + self.leaderId}'
        with grpc.insecure_channel(leader_ip) as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            response = stub.ServeClient(raft_pb2.ServeClientRequest(request = f'SET {key} {value}'))
            try:
                if response.leaderId:
                    if response.leaderId == "NONE":
                        time.sleep(0.1)
                        return self.set(key, value)
                    self.leaderId = response.leaderId
                    return self.set(key, value)
            except AttributeError:
                if response.success:
                    return "OK"