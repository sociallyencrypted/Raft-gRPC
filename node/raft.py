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
            parts = last_log.strip().split(':')
            last_log_term = int(parts[0])
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
            if request.term < self.currentTerm:
                # Reply with failure if the term is outdated
                return raft_pb2.AppendEntriesReply(
                    term=self.currentTerm,
                    success=False,
                    conflictingTerm=0,
                    conflictingIndex=0
                )

            # Reset the election timer on receiving the heartbeat
            self.reset_election_timer()
            # print("This should be working")
            print(f"Heartbeat received from leader {request.leaderId} for term {request.term}")

            # Send successful reply
            return raft_pb2.AppendEntriesReply(
                term=self.currentTerm,
                success=True,
                conflictingTerm=0,
                conflictingIndex=0
            )

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
        print(f"Leader Node {self.node_id} sending heartbeat to followers")
        self.reacquire_lease()
        for address in self.node_addresses:
            # try to get acks from majority of nodes. if fail, step down as leader: TO BE DONE
            total_acks = 0
            if address != f'localhost:{50050 + self.node_id}':
                try:
                    channel = grpc.insecure_channel(address)
                    stub = raft_pb2_grpc.RaftNodeStub(channel)
                    stub.AppendEntries(raft_pb2.AppendEntriesRequest(
                        term=self.currentTerm,
                        leaderId=self.node_id,
                        prevLogIndex=0,
                        prevLogTerm=0,
                        entries=[],
                        leaderCommit=0,
                        leaseDuration = LEASE_DURATION
                    ))
                    total_acks += 1
                except Exception as e:
                    print(f"Failed to send heartbeat to {address}: {e}")
                    
            if total_acks > len(self.node_addresses) // 2:
                self.become_leader()
                return
            else:
                print(f"Failed to get majority acks from followers. Total acks: {total_acks}")
                self.role = 'follower'
                self.leaderId = None
                self.initiate_election()
                return

        # Reschedule the heartbeat
        self.heartbeat_timer = threading.Timer(1, self.send_heartbeat)
        self.heartbeat_timer.start()
        
    def reacquire_lease(self):
        # restart the lease timer
        if self.leaseTimer is not None:
            self.leaseTimer.cancel()
        self.leaseTimer = threading.Timer(LEASE_DURATION, self.reacquire_lease)
        self.leaseTimer.start()


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
                        self.replicate_log(node, key, value)
                return raft_pb2.ServeClientReply(success=True) # Return "OK" to the client
        else:
            if self.leaderId:
                return raft_pb2.ServeClientReply(leaderId=self.leaderId) # Return the leaderId to the client
            else:
                return raft_pb2.ServeClientReply(leaderId="NONE") # Return "NONE" if there is no leader
            

# if __name__ == "__main__":
#     # Example code to start a RaftNode
#     # You might need to adjust this based on how you initialize your nodes and their configurations
#     node_id = 1  # Node ID should be unique for each node
#     node_addresses = ['localhost:50051', 'localhost:50052', 'localhost:50053']  # List of addresses for all nodes in the cluster
#     raft_node = RaftNode(node_id, node_addresses)
#     # Removed the call to serve() as per your instructions

