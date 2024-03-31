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
        self.commitIndex = 0


    def start_election_timeout(self):
        self.electionTimer = Timer(random.uniform(5, 10), self.initiate_election)
        self.electionTimer.start()

    def initiate_election(self):
        if self.role == 'leader':  # Prevent initiating election if already a leader
            return
        self.role = "candidate"
        self.currentTerm += 1
        self.votedFor = self.node_id  # Node votes for itself
        print(f"Node {self.node_id} becoming candidate for term {self.currentTerm}")
        self.request_votes_from_peers()

    def request_votes_from_peers(self):
        lastLogIndex, lastLogTerm = self.get_last_log_info()
        self.vote_count = 1
        self.response_count = 0
        self.majority_received_event = threading.Event()

        request = raft_pb2.RequestVoteRequest(
            term=self.currentTerm,
            candidateId=self.node_id,
            lastLogIndex=lastLogIndex,
            lastLogTerm=lastLogTerm
        )

        for i in self.node_addresses:
            if i == f'localhost:{50050+self.node_id}':
                continue

            try:
                channel = grpc.insecure_channel(i, options=[('grpc.enable_http_proxy', 0)])
                stub = raft_pb2_grpc.RaftNodeStub(channel)
                response = stub.RequestVote(request, timeout=2)
                self.response_count += 1

                if response.voteGranted:
                    self.process_vote_response()
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print("Error: Service unavailable. Check server status or network connection.")
                elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    print("Error: Deadline exceeded. Retry the operation.")
                else:
                    print("Error:", e.details())

             # Start a timer to check for majority vote
        majority_check_timer = threading.Timer(5, self.check_majority_votes)
        majority_check_timer.start()

    def process_vote_response(self):
        with self.vote_count_lock:
            self.vote_count += 1
            if self.vote_count > len(self.node_addresses) // 2:
                self.majority_received_event.set()
                self.become_leader()

    def check_majority_votes(self):
        with self.vote_count_lock:
            if not self.majority_received_event.is_set():
                if self.response_count < len(self.node_addresses) // 2 + 1 or self.vote_count <= len(self.node_addresses) // 2:
                    self.become_follower()

    def become_follower(self):
        self.role = 'follower'
        self.leaderId = ""
        self.votedFor = None
        self.print_and_dump(f"Node {self.node_id} reverting to follower role")
        self.restart_election_timer()

    def handle_vote_response(self, response):
        if response and response.voteGranted:
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
        
    def restart_election_timer(self):
        if self.electionTimer is not None:
            self.electionTimer.cancel()
        self.electionTimer = Timer(random.uniform(5, 10), self.initiate_election)
        self.electionTimer.start()

    def reset_election_timer(self):
        if self.electionTimer is not None:
            self.electionTimer.cancel()
            self.electionTimer.cancel()
        # print("resetting_election")
        self.electionTimer = Timer(random.uniform(5, 10), self.initiate_election)
        self.electionTimer.start()

    def RequestVote(self, request, context):
        with self.vote_count_lock:
            if request.term < self.currentTerm:
                return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=False)

            if request.term > self.currentTerm:
                self.currentTerm = request.term
                self.become_follower(request.candidateId)

            # Additional check for split vote scenario
            if self.role == "candidate" and request.term == self.currentTerm:
                return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=False)

            if (self.votedFor is None or self.votedFor == request.candidateId) and self.is_log_up_to_date(request.lastLogIndex, request.lastLogTerm):
                self.votedFor = request.candidateId
                self.reset_election_timer()
                print(f"Node {self.node_id} granted vote to Node {request.candidateId} for term {self.currentTerm}")
                return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=True)
            else:
                return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=False)

    def become_follower(self, leader_id=None):
        self.role = 'follower'
        self.leaderId = leader_id
        self.votedFor = None
        self.restart_election_timer()
        
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
                while conflictingIndex > self.commitIndex and int(self.storage.logs[conflictingIndex].split(" ")[-1]) == conflictingTerm:
                    conflictingIndex -= 1
                return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=False, conflictingIndex=max(self.commitIndex+1, conflictingIndex), conflictingTerm=conflictingTerm)
            
            # Append the entries to the log
            self.update_follower_logs(request.prevLogIndex, request.leaderCommit, request.entries)
            self.print_and_dump(f"Node {self.node_id} accepted AppendEntries request from leader {request.leaderId}.")
            # Reset the election timer on receiving the heartbeat
            self.reset_election_timer()

            # Send successful reply
            return raft_pb2.AppendEntriesReply(
                term=self.currentTerm,
                success=True,
            )
        
    def update_follower_logs(self, prevLogIndex, leaderCommit, entries):
        for i in range(len(entries)):
            if prevLogIndex + i + 1 >= len(self.storage.logs)-1:
                break
            if self.storage.logs[prevLogIndex + i+1].split(" ")[-1] != entries[i].term:
                self.storage.logs = self.storage.logs[:prevLogIndex + i+1] 
        for entry in entries:
            if entry.operation == "NO-OP":
                self.storage.append_log(entry.operation, entry.term)
            else:
                key = entry.operation.split(" ")[1]
                value = entry.operation.split(" ")[2]
                self.storage.append_log("SET", entry.term, key, value)
        self.commitIndex= max(self.commitIndex, min(leaderCommit, prevLogIndex + len(entries)))
        # commit entries upto commitIndex
        for i in range(prevLogIndex+i+1, self.commitIndex+1):
            self.apply_log(i)  
                    
    def apply_log(self, index):
        log = self.storage.logs[index]
        log = log.split(" ")
        log_type = log[0]
        if log_type == 'SET':
            self.storage.state[log[1]] = log[2]
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

        total_acks = 1  # Count self as acked
        for address in self.node_addresses:
            if address != f'localhost:{50050 + self.node_id}':
                ack_success = self.replicate_log(address)
                if ack_success:
                    total_acks += 1

        if total_acks <= (len(self.node_addresses)-1) // 2:
            print(f"Failed to get majority acks from followers. Total acks: {total_acks}")
            self.role = 'follower'
            self.leaderId = None
            self.restart_election_timer()
            return

        self.reacquire_lease()
        # commit all entries till now
        for i in range(self.commitIndex+1, len(self.storage.logs)):
            self.apply_log(i)
        self.commitIndex = len(self.storage.logs) - 1

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
        try:
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
            response = stub.AppendEntries(raft_pb2.AppendEntriesRequest(
                term=self.currentTerm,
                leaderId=self.node_id,
                prevLogIndex=prevIndex,
                prevLogTerm=prevTerm,
                entries=entries,
                leaderCommit=self.commitIndex,
                leaseDuration = LEASE_DURATION
            ))
            
            
            if response.success:
                if prevIndex + len(entries) >= self.nextIndex[node_id]:
                    self.nextIndex[node_id] = prevIndex + len(entries) + 1
                    print(f" Updated nextIndex for node {node_id} to {self.nextIndex[node_id]}")
                    self.matchIndex[node_id] = prevIndex + len(entries)
                    if len(entries) > 0:
                        self.storage.write_to_dump(f"Node {self.node_id} successfully replicated log to {node}")
                    return True
                if prevIndex + len(entries) < len(self.storage.logs) - 1 and self.commitIndex < prevIndex + len(entries) and int(self.storage.logs[prevIndex + len(entries)].split(" ")[-1]) == self.currentTerm:
                    self.commitIndex = prevIndex + len(entries)
                    self.storage.write_to_dump(f"Node {self.node_id} successfully replicated log to {node}")
                    
            else:
                print(f"Node {self.node_id} failed to replicate log to {node}")
                conflictingIndex = response.conflictingIndex
                conflictingTerm = response.conflictingTerm
                if conflictingTerm > self.currentTerm:
                    self.currentTerm = conflictingTerm
                    self.become_follower()
                else:
                    self.nextIndex[node_id] = max(1, min(conflictingIndex, self.nextIndex[node_id] - 1))
                    print(f" Updated nextIndex for node {node_id} to {self.nextIndex[node_id]}")
                return False
        except grpc.RpcError as e:
            # print(f"Failed to connect to {node}, error: {e}")
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                print("Error: Service unavailable. Check server status")
            elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                print("Error: Deadline exceeded. Retry")
            return False

