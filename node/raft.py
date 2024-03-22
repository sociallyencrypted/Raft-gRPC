import grpc
import raft_pb2, raft_pb2_grpc
from node.storage import Storage
from concurrent import futures
import random
import threading
from threading import Lock

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
        self.start_election_timeout()

    def start_election_timeout(self):
        self.electionTimer = threading.Timer(random.uniform(5, 10), self.initiate_election)
        self.electionTimer.start()

    def initiate_election(self):
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

    def become_leader(self):
        self.role = 'leader'
        print(f"Node {self.node_id} is now the leader for term {self.currentTerm}")
        # Additional logic for becoming a leader

    def get_last_log_info(self):
        if self.storage.logs:
            last_log = self.storage.logs[-1]
            parts = last_log.strip().split(':')
            last_log_term = int(parts[0])  
            last_log_index = len(self.storage.logs) - 1
            return last_log_index, last_log_term
        else:
            return 0, 0

    def reset_election_timer(self):
        if self.electionTimer is not None:
            self.electionTimer.cancel()
        self.electionTimer = threading.Timer(random.uniform(5, 10), self.initiate_election)
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

    # Implement AppendEntries, ServeClient, etc.

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftNodeServicer_to_server(self, server)
        server.add_insecure_port(f'localhost:{50050 + self.node_id}')
        server.start()
        print(f"Server started on port {50050 + self.node_id}")
        self.start_election_timeout()
        server.wait_for_termination()

    def AppendEntries(self, request, context):
        # Implement the logic for handling AppendEntries RPC calls
        # This method is used for replicating log entries and performing heartbeats
        pass

    def ServeClient(self, request, context):
        # Implement the logic for handling client requests
        # This is where you handle client interactions like setting a key-value or retrieving a value
        pass

    # Any additional methods or helpers required for the Raft protocol can go here

if __name__ == "__main__":
    # Example code to start a RaftNode
    # You might need to adjust this based on how you initialize your nodes and their configurations
    node_id = 1  # Node ID should be unique for each node
    node_addresses = ['localhost:50051', 'localhost:50052', 'localhost:50053']  # List of addresses for all nodes in the cluster
    raft_node = RaftNode(node_id, node_addresses)
    # Removed the call to serve() as per your instructions

