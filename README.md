# Raft Consensus Algorithm Implementation

This project implements the Raft consensus algorithm in Python using gRPC for network communication. Raft is a distributed consensus algorithm designed to be more understandable than Paxos while providing the same fault-tolerance and performance.

## Features

- Leader election
- Log replication
- Safety (State Machine Safety)
- Fault tolerance
- Persistent state storage
- Client request handling (GET/SET operations)

## Components

### RaftNode

The main class implementing the Raft algorithm. Key functionalities include:

- Managing node states (follower, candidate, leader)
- Handling leader election
- Processing AppendEntries requests
- Replicating logs
- Serving client requests

### Storage

Handles persistent storage of logs and metadata, including:

- Appending and retrieving logs
- Updating and retrieving metadata
- Managing the state machine

### gRPC Services

Defines RPC methods for inter-node communication:

- AppendEntries: Used for log replication and heartbeats
- RequestVote: Used in the leader election process
- ServeClient: Handles client requests (GET/SET operations)

## Requirements

- Python 3.7+
- gRPC
- protobuf

## Installation

1. Clone the repository:

git clone https://github.com/yourusername/raft-consensus.git
cd raft-consensus

2. Install the required packages:

pip install grpcio grpcio-tools

3. Generate gRPC code:

python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto

## Usage

1. Start a Raft node:

python raft_node.py <node_id> <node1_address> <node2_address> ...

2. Interact with the cluster using a client:

python client.py <node_address>

## Configuration

The `config.ini` file contains configuration parameters:

- `LEASE_DURATION`: The duration of the leader's lease

## Key Concepts

- **Election Timeout**: Randomized timer to initiate leader election
- **Heartbeat**: Regular AppendEntries requests sent by the leader
- **Log Replication**: Process of synchronizing logs across nodes
- **Commit Index**: The highest log entry known to be committed
- **Term**: Logical clock that increases monotonically
