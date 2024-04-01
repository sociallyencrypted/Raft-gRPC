import argparse
import os
from node.raft import RaftNode
from client.client import RaftClient
import  random

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--node', action='store_true', help='Run as a Raft node')
    parser.add_argument('--client', action='store_true', help='Run as a client')
    args = parser.parse_args()

    if args.client:
        # Run as client
        node_addresses = get_node_addresses()
        client = RaftClient(node_addresses)
        client.set("key1", "value1")
        print(client.get("key1"))
        # Interact with the client
    elif args.node:
        # Run as Raft node
        node_id = int(input("Enter Node ID"))
        node_addresses = get_node_addresses()
        node = RaftNode(node_id, node_addresses)
        node.serve()
        # Start gRPC server for the node

def get_node_id():
    # node_id_file = os.environ.get('NODE_ID_FILE')
    # with open(node_id_file, 'r') as f:
    #     node_id = int(f.read().strip())
    # return node_id
    return random.randint(1, 10)

def get_node_addresses():
    node_addresses = []
    for i in range(1, 4):  # Assume a maximum of 10 nodes
        node_address = f'localhost:{50050 + i}'
        node_addresses.append(node_address)
    return node_addresses

if __name__ == '__main__':
    main()
