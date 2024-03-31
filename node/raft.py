import grpc
import raft_pb2, raft_pb2_grpc
from node.storage import Storage
from concurrent import futures
import random
import threading
from threading import Lock, Timer
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')
LEASE_DURATION = int(config['DEFAULT']['LEASE_DURATION'])

class RaftNode(raft_pb2_grpc.RaftNodeServicer):
    def __init__(self, node_id, node_addresses):
        self.node_id = node_id
        self.node_addresses = node_addresses
        self.storage = Storage(node_id)
        self.role = "follower"
        self.leaderId = ""
        self.currentTerm = 0
        self.votedFor = None
        self.electionTimer = None
        self.vote_count = 1
        self.vote_count_lock = Lock()
        self.leaseTimer = None
        self.nextIndex = {}
        self.matchIndex = {}


    def start_election_timeout(self):
        self.electionTimer = Timer(random.uniform(5, 10), self.initiate_election)
        self.electionTimer.start()

    def initiate_election(self):
        if self.role == 'leader':  # Prevent initiating election if already a leader
            return
        self.role = "candidate"
        self.currentTerm += 1
        self.votedFor = self.node_id
        print(f"Node {self.node_id} becoming candidate for term {self.currentTerm}")
        self.request_votes_from_peers()

    def request_votes_from_peers(self):
        lastLogIndex, lastLogTerm = self.get_last_log_info()
        self.vote_count = 1  # Reset vote count for new election term

        for address in self.node_addresses:
            if address != f'localhost:{50050 + self.node_id}':
                channel = grpc.insecure_channel(address)
                stub = raft_pb2_grpc.RaftNodeStub(channel)
                vote_request = raft_pb2.RequestVoteRequest(
                    term=self.currentTerm,
                    candidateId=self.node_id,
                    lastLogIndex=lastLogIndex,
                    lastLogTerm=lastLogTerm
                )
                future = stub.RequestVote.future(vote_request)
                future.add_done_callback(
                    lambda response_future: self.handle_vote_response(response_future.result())
                )

    def handle_vote_response(self, response):
        if response.voteGranted:
            with self.vote_count_lock:
                self.vote_count += 1
                print(f"Node {self.node_id} received vote in term {self.currentTerm}. Total votes: {self.vote_count}")
                if self.vote_count > len(self.node_addresses) // 2 and self.role == 'candidate':
                    self.become_leader()

    def get_last_log_info(self):
        if self.storage.logs:
            last_log = self.storage.logs[-1]
            parts = last_log.split(" ")
            last_log_term = int(parts[-1])
            last_log_index = len(self.storage.logs) - 1
            return last_log_index, last_log_term
        else:
            return 0, 0

    def reset_election_timer(self):
        if self.electionTimer is not None:
            self.electionTimer.cancel()
        print("resetting_election")
        self.electionTimer = Timer(random.uniform(5, 10), self.initiate_election)
        self.electionTimer.start()

    def RequestVote(self, request, context):
        with self.vote_count_lock:
            if request.term < self.currentTerm:
                return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=False)

            if (self.votedFor is not None and self.votedFor != request.candidateId) or not self.is_log_up_to_date(request.lastLogIndex, request.lastLogTerm):
                return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=False)

            self.votedFor = request.candidateId
            self.currentTerm = request.term
            self.reset_election_timer()
            print(f"Node {self.node_id} granted vote to Node {request.candidateId} for term {self.currentTerm}")
            return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=True)

    def is_log_up_to_date(self, lastLogIndex, lastLogTerm):
        localLastLogIndex, localLastLogTerm = self.get_last_log_info()
        return not (lastLogTerm < localLastLogTerm or (lastLogTerm == localLastLogTerm and lastLogIndex < localLastLogIndex))

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftNodeServicer_to_server(self, server)
        server.add_insecure_port(f'localhost:{50050 + self.node_id}')
        server.start()
        print(f"Server started on port {50050 + self.node_id}")
        self.start_election_timeout()
        server.wait_for_termination()

    def AppendEntries(self, request, context):
        with self.vote_count_lock:
            
            if self.currentTerm > request.term:
                return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=False)
            
            self.leaderId = request.leaderId
            
            self.reset_election_timer()
            
            if request.term > self.currentTerm:
                self.currentTerm = request.term
                self.votedFor = None
            
            self.role = "follower"
            print(f"Node {self.node_id} received AppendEntries request from leader {request.leaderId}")
            
            if len(self.storage.logs) == 0:
                # accept request
                self.update_follower_logs(request.prevLogIndex, request.leaderCommit, request.entries)
                self.print_and_dump(f"Node {self.node_id} accepted AppendEntries request from leader {request.leaderId}.")
                self.reset_election_timer()
                return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=True)    
            
            print(f"PrevLogIndex: {request.prevLogIndex}, PrevLogTerm: {request.prevLogTerm}")
            print(f"Self.PrevLogIndex: {len(self.storage.logs)-1}, Self.PrevLogTerm: {int(self.storage.logs[-1].split(' ')[-1])}")       
            
            if request.prevLogIndex > (len(self.storage.logs)-1) or int(self.storage.logs[request.prevLogIndex].split(" ")[-1]) != request.prevLogTerm:
                conflictingIndex = min(request.prevLogIndex, len(self.storage.logs)-1)
                conflictingTerm = int(self.storage.logs[conflictingIndex].split(" ")[-1])
                while conflictingIndex > 0 and int(self.storage.logs[conflictingIndex].split(" ")[-1]) == conflictingTerm:
                    conflictingIndex -= 1
                return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=False, conflictingIndex=conflictingIndex, conflictingTerm=conflictingTerm)
            
            # Append the entries to the log
            self.update_follower_logs(request.prevLogIndex, request.leaderCommit, request.entries)
            print(f"entries: {request.entries}")
            self.print_and_dump(f"Node {self.node_id} accepted AppendEntries request from leader {request.leaderId}.")
            # Reset the election timer on receiving the heartbeat
            self.reset_election_timer()

            # Send successful reply
            return raft_pb2.AppendEntriesReply(
                term=self.currentTerm,
                success=True,
            )
        
    def update_follower_logs(self, prevLogIndex, leaderCommit, entries):
        # to be done. for now, just append the entries
        own_prev_log_index = len(self.storage.logs) - 1
        entries = entries[own_prev_log_index + 1:]
        for entry in entries:
            if entry.operation == "NO-OP":
                self.storage.append_log(entry.operation, entry.term)
            else:
                key = entry.operation.split(" ")[1]
                value = entry.operation.split(" ")[2]
                self.storage.append_log("SET", entry.term, key, value)
        if leaderCommit == 1:
            for i in range(own_prev_log_index + 1, len(self.storage.logs)):
                self.apply_log(i)
        
        
                    
                    
    def apply_log(self, index):
        log = self.storage.logs[index]
        if log.type == 'SET':
            self.storage.state[log.key] = log.value
        self.storage.write_to_dump(f"Node {self.node_id} committed the entry {log} to the state machine")        

    def become_leader(self):
        self.role = 'leader'
        self.leaderId = self.node_id
        self.print_and_dump(f"Node {self.node_id} became the leader for term {self.currentTerm}")
        # Cancel the election timer as this node is now the leader
        if self.electionTimer is not None:
            self.electionTimer.cancel()
        # Start the heartbeat process
        self.start_heartbeat()
        
    def print_and_dump(self, statement):
        print(statement)
        self.storage.write_to_dump(statement)
        
    def start_heartbeat(self):
        self.heartbeat_timer = threading.Timer(1, self.send_heartbeat)
        self.heartbeat_timer.start()

    def send_heartbeat(self):
        if self.role != 'leader':
            return
        self.print_and_dump(f"Leader {self.node_id} sending heartbeat & Renewing Lease")
        
        # try to get acks from majority of nodes. if fail, step down as leader: TO BE DONE
        total_acks = 0
        for address in self.node_addresses:
            if address != f'localhost:{50050 + self.node_id}':
                self.replicate_log(address)
                total_acks += 1
                    
        if total_acks < len(self.node_addresses) // 2:
            print(f"Failed to get majority acks from followers. Total acks: {total_acks}")
            self.role = 'follower'
            self.leaderId = None
            self.initiate_election()
            return
            
        self.reacquire_lease()

        # Reschedule the heartbeat
        self.heartbeat_timer = threading.Timer(1, self.send_heartbeat)
        self.heartbeat_timer.start()
        
    def reacquire_lease(self):
        # restart the lease timer
        if self.leaseTimer is not None:
            self.leaseTimer.cancel()
        self.leaseTimer = threading.Timer(LEASE_DURATION, self.reacquire_lease)
        self.leaseTimer.start()
        self.storage.append_log("NO-OP", self.currentTerm)


    def ServeClient(self, request, context):
        request = request.request.split(" ")
        if request[0] == "GET":
            if self.role == "leader":
                key = request[1] # Get the key from the request
                if key == "LEADER":
                    return raft_pb2.ServeClientReply(leaderId=self.node_id)
                value = self.storage.get(key)
                if value is None:
                    # set context.status to NOT_FOUND
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details("Key not found")
                    # send back the context
                    return raft_pb2.ServeClientReply()
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
                self.storage.append_log("SET", self.currentTerm, key, value)
                for node in self.node_addresses:
                    if node != f'localhost:{50050 + self.node_id}':
                        self.replicate_log(node)
                return raft_pb2.ServeClientReply(success=True) # Return "OK" to the client
        else:
            if self.leaderId:
                return raft_pb2.ServeClientReply(leaderId=self.leaderId) # Return the leaderId to the client
            else:
                return raft_pb2.ServeClientReply(leaderId="NONE") # Return "NONE" if there is no leader
            
    def replicate_log(self, node):
        # to be done. for now, send an empty heartbeat
        channel = grpc.insecure_channel(node)
        stub = raft_pb2_grpc.RaftNodeStub(channel)
        node_id = int(node.split(":")[-1]) - 50050
        if node_id not in self.nextIndex:
            self.nextIndex[node_id] = 0
        prevIndex = self.nextIndex[node_id] - 1
        if prevIndex < 0:
            prevTerm = 0
        else:
            prevTerm = int(self.storage.logs[prevIndex].split(" ")[-1])
        # entries i self.storage.logs[prevIndex+1:]
        entries = []
        for i in range(prevIndex+1, len(self.storage.logs)):
            op = " ".join(self.storage.logs[i].split(" ")[:-1])
            trm = int(self.storage.logs[i].split(" ")[-1])
            entry = raft_pb2.LogEntry(operation=op, term=trm)
            entries.append(entry)
        try:
            response = stub.AppendEntries(raft_pb2.AppendEntriesRequest(
                term=self.currentTerm,
                leaderId=self.node_id,
                prevLogIndex=prevIndex,
                prevLogTerm=prevTerm,
                entries=entries,
                leaderCommit=0,
                leaseDuration = LEASE_DURATION
            ))
            
            
            if response.success:
                if prevIndex + len(entries) >= self.nextIndex[node_id]:
                    self.nextIndex[node_id] = prevIndex + len(entries) + 1
                    print(f" Updated nextIndex for node {node_id} to {self.nextIndex[node_id]}")
                    self.matchIndex[node_id] = prevIndex + len(entries)
                    if len(entries) > 0:
                        self.storage.write_to_dump(f"Node {self.node_id} successfully replicated log to {node}")
            else:
                print(f"Failed to replicate log to {node}")
                if self.nextIndex[node_id] > 0:
                    self.nextIndex[node_id] -= 1
                print(f" Updated nextIndex for node {node_id} to {self.nextIndex[node_id]}")
        except Exception as e:
            print(f"Failed to replicate log to {node}. Error: {e}")
        
            

# if __name__ == "__main__":
#     # Example code to start a RaftNode
#     # You might need to adjust this based on how you initialize your nodes and their configurations
#     node_id = 1  # Node ID should be unique for each node
#     node_addresses = ['localhost:50051', 'localhost:50052', 'localhost:50053']  # List of addresses for all nodes in the cluster
#     raft_node = RaftNode(node_id, node_addresses)
#     # Removed the call to serve() as per your instructions

