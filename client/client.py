import grpc
import raft_pb2, raft_pb2_grpc
import time

class RaftClient:
    def __init__(self, node_addresses):
        self.node_addresses = node_addresses
        self.leaderId = None
        self.leaderIP = None
        self.update_leader()
        
    def update_leader(self):
        self.leaderId = self.get_leader()
        if self.leaderId is None:
            print("No leader found")
            exit(1)
        self.leaderIP = f'localhost:{50050 + self.leaderId}'
        
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
                        print("No leader found")
            except:
                continue
        return None
        

    def get(self, key):
        with grpc.insecure_channel(self.leaderIP) as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            print(f"Sending GET request for {key} to leader at {self.leaderIP}")
            response = stub.ServeClient(raft_pb2.ServeClientRequest(request = f'GET {key}'))
            try:
                if response.data:
                    return response.data
                else:
                    return "Key not found"
            except AttributeError:
                if response.leaderId:
                    if response.leaderId == "NONE":
                        time.sleep(0.1)
                        return self.get(key)
                    self.leaderId = response.leaderId
                    return self.get(key)
                

    def set(self, key, value):
        with grpc.insecure_channel(self.leaderIP) as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            print(f"Sending SET request for {key}:{value} to leader at {self.leaderIP}")
            response = stub.ServeClient(raft_pb2.ServeClientRequest(request = f'SET {key} {value}'))
            try:
                if response.success:
                    return "OK"
                else:
                    return "Error"
            except AttributeError:
                if response.leaderId:
                    if response.leaderId == "NONE":
                        time.sleep(0.1)
                        return self.set(key, value)
                    self.leaderId = response.leaderId
                    return self.set(key, value)
