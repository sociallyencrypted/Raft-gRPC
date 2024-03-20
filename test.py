from node.raft import RaftNode
from client.client import RaftClient
import random
import time

def main():
    
    node_id = get_node_id()
    node_addresses = get_node_addresses()
    node = RaftNode(node_id, node_addresses)
    
    time.sleep(5)
    
    node_addresses = get_node_addresses()
    client = RaftClient(node_addresses)
    
    # set a key-value pair
    key = "key1"
    value = "value1"
    
    response = client.set(key, value)
    print(response)
    
    # get the value for the key
    response = client.get(key)
    print(response)

def get_node_id():
    return random.randint(1, 10)

def get_node_addresses():
    node_addresses = []
    for i in range(1, 11):  # Assume a maximum of 10 nodes
        node_address = f'localhost:{50050 + i}'
        node_addresses.append(node_address)
    return node_addresses

if __name__ == '__main__':
    main()