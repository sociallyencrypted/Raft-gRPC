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
        if self.storage.metadata['currentTerm'] is not None:
            self.currentTerm = int(self.storage.metadata['currentTerm'])
        if self.storage.metadata['votedFor'] is not None:
            self.votedFor = int(self.storage.metadata['votedFor'])
        if self.storage.metadata['commitIndex'] is not None:
            self.commitIndex = int(self.storage.metadata['commitIndex'])

    def start_election_timeout(self):
        self.electionTimer = Timer(random.uniform(5, 10), self.initiate_election)
        self.electionTimer.start()

    def initiate_election(self):
        if self.role == 'leader':  # Prevent initiating election if already a leader
            return
        self.print_and_dump(f"Node {self.node_id} election timer timed out, Starting election.")
        self.role = "candidate"
        self.currentTerm += 1
        self.storage.update_metadata('currentTerm', self.currentTerm)
        self.votedFor = self.node_id  # Node votes for itself
        self.storage.update_metadata('votedFor', self.node_id)
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
        self.storage.update_metadata('votedFor', self.node_id)
        self.print_and_dump(f"{self.node_id} Stepping down")
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
                self.storage.update_metadata('currentTerm', self.currentTerm)
                self.become_follower(request.candidateId)

            # Additional check for split vote scenario
            if self.role == "candidate" and request.term == self.currentTerm:
                return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=False)

            if (self.votedFor is None or self.votedFor == request.candidateId) and self.is_log_up_to_date(request.lastLogIndex, request.lastLogTerm):
                self.votedFor = request.candidateId
                self.storage.update_metadata('votedFor', self.votedFor)  # Corrected this line
                self.reset_election_timer()
                self.print_and_dump(f"Vote granted for Node {request.candidateId} in term {self.currentTerm}")
                return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=True)
            else:
                self.print_and_dump(f"Vote denied for Node {request.candidateId} in term {self.currentTerm}")
                return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=False)

    def become_follower(self, leader_id=None):
        self.role = 'follower'
        self.leaderId = leader_id
        self.votedFor = None
        self.storage.update_metadata('votedFor', self.node_id)
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
                self.storage.update_metadata('currentTerm', self.currentTerm)
                self.votedFor = None
                self.storage.update_metadata('votedFor', None)
            
            self.role = "follower"
            print(f"Node {self.node_id} received AppendEntries request from leader {request.leaderId}")
            
            if len(self.storage.logs) == 0:
                # accept request
                self.update_follower_logs(request.prevLogIndex, request.leaderCommit, request.entries)
                self.print_and_dump(f"Node {self.node_id} accepted AppendEntries request from leader {request.leaderId}.")
                self.reset_election_timer()
                return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=True)       
            
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
        print(f"DEBUG: prevLogIndex: {prevLogIndex}, lenth of logs: {len(self.storage.logs)}")
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
        self.storage.update_metadata('commitIndex', self.commitIndex)
        # commit entries upto commitIndex
        for i in range(prevLogIndex+1, self.commitIndex+1):
            print(f"DEBUG: COMMITTING LOG {i}")
            self.apply_log(i)  
                    
    def apply_log(self, index):
        log = self.storage.logs[index]
        log = log.split(" ")
        log_type = log[0]
        if log_type == 'SET':
            self.storage.state[log[1]] = log[2]
        self.print_and_dump(f"Node {self.node_id} ({self.role}) committed the entry {log} to the state machine")        

    def become_leader(self):
        self.role = 'leader'
        self.leaderId = self.node_id
        self.print_and_dump("New Leader waiting for Old Leader Lease to timeout.")
        # print(f"DEBUG: Time left: {self.leaseTimer.interval}")
        # wait till lease timer expires
        # self.leaseTimer.finished.wait()
        self.print_and_dump(f"Node {self.node_id} became the leader for term {self.currentTerm}")
        # Cancel the election timer as this node is now the leader
        if self.electionTimer is not None:
            self.electionTimer.cancel()
        # Start the heartbeat process
        self.reacquire_lease()
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

        if total_acks <= (len(self.node_addresses) - 1) // 2:
            self.print_and_dump(f"Leader {self.node_id} lease renewal failed. Stepping Down.")
            self.role = 'follower'
            self.leaderId = None
            self.restart_election_timer()
            return

        self.reacquire_lease()

        # Commit only the entries that have been replicated to a majority
        majority_commit_index = self.get_majority_commit_index()
        if majority_commit_index > self.commitIndex:
            self.print_and_dump(f"Node {self.node_id} committing entries up to index {majority_commit_index}")
            for i in range(self.commitIndex + 1, majority_commit_index + 1):
                self.apply_log(i)
            self.commitIndex = majority_commit_index
            self.storage.update_metadata('commitIndex', self.commitIndex)

        # Reschedule the heartbeat
        self.heartbeat_timer = threading.Timer(1, self.send_heartbeat)
        self.heartbeat_timer.start()

    def get_majority_commit_index(self):
        """
        Determine the highest log index that has been replicated to a majority of the cluster.
        """
        commit_counts = [0] * (len(self.storage.logs))
        for node_id, match_index in self.matchIndex.items():
            for i in range(match_index + 1):
                commit_counts[i] += 1

        for i in range(len(self.storage.logs) - 1, -1, -1):
            if commit_counts[i] > (len(self.node_addresses) - 1) // 2:
                return i

        return self.commitIndex
        
    def reacquire_lease(self):
        # restart the lease timer
        if self.leaseTimer is not None:
            self.leaseTimer.cancel()
        self.leaseTimer = threading.Timer(LEASE_DURATION, self.reacquire_lease)
        self.leaseTimer.start()
        self.storage.append_log("NO-OP", self.currentTerm)


    def ServeClient(self, request, context):
        self.print_and_dump(f"Node {self.node_id} (leader) received a {request.request} request.")
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
                self.print_and_dump(f"Node {self.node_id} (leader) served GET request for key {key}")
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
        node_id = int(node.split(":")[-1]) - 50050
        try:
            channel = grpc.insecure_channel(node)
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            if node_id not in self.nextIndex:
                self.nextIndex[node_id] = len(self.storage.logs)
            prevIndex = self.nextIndex[node_id] - 1
            if prevIndex < 0:
                prevTerm = 0
            else:
                prevTerm = int(self.storage.logs[prevIndex].split(" ")[-1])
            entries = []
            for i in range(self.nextIndex[node_id], len(self.storage.logs)):
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
                leaseDuration=LEASE_DURATION
            ))

            if response.success:
                self.nextIndex[node_id] = len(self.storage.logs)
                self.matchIndex[node_id] = len(self.storage.logs) - 1
                if len(entries) > 0:
                    self.print_and_dump(f"Node {self.node_id} successfully replicated log to {node}")

                # Commit the entries that have been replicated
                for i in range(self.nextIndex[node_id] - len(entries), self.nextIndex[node_id]):
                    if i > self.commitIndex:
                        self.commitIndex = i
                        self.storage.update_metadata('commitIndex', self.commitIndex)
                        self.print_and_dump(f"Node {self.node_id} successfully committed log to {node}")
                        self.apply_log(i)
                return True
            else:
                print(f"Node {self.node_id} failed to replicate log to {node}")
                conflictingIndex = response.conflictingIndex
                conflictingTerm = response.conflictingTerm
                if conflictingTerm > self.currentTerm:
                    self.currentTerm = conflictingTerm
                    self.storage.update_metadata('currentTerm', self.currentTerm)
                    self.become_follower()
                else:
                    self.nextIndex[node_id] = max(1, min(conflictingIndex, self.nextIndex[node_id] - 1))
                return False
        except grpc.RpcError as e:
            self.print_and_dump(f"Error occurred while sending RPC to Node {node_id}")
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                print("Error: Service unavailable. Check server status")
            elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                print("Error: Deadline exceeded. Retry")
            else:
                print("Error:", e.details())
            return False

