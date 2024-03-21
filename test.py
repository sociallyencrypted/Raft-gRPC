from node.raft import RaftNode
from client.client import RaftClient
import random
import time
import threading

node_addresses=[]

def main():
    
    node_id = get_node_id()
    node_addresses.append(f'localhost:{50050 + node_id}')
    node = RaftNode(node_id, node_addresses)
    threading.Thread(target=node.serve).start()
    time.sleep(1)
    
    client = RaftClient(node_addresses)
    
    # set a key-value pair
    key = "key1"
    value = "value1"
    
    response = client.set(key, value)
    print(response)
    
    # get the value for the key
    response = client.get(key)
    print(response)
    
    response = client.get("key2")
    print(response)

def get_node_id():
    return 4

if __name__ == '__main__':
    main()