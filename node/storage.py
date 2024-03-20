import os

class Storage:
    def __init__(self, node_id):
        self.node_id = node_id
        self.logs = []
        self.metadata = {}
        self.load_state()

    def append_log(self, key, value):
        self.metadata[key] = value
        entry = f'{key}:{value}\n'
        f = open(f'logs_{self.node_id}.txt', 'a')
        f.write(entry)
        f.close()

    def dump_state(self):
        f = open(f'logs_{self.node_id}.txt', 'w')
        for log in self.logs:
            f.write(log)
        f.close()
        f = open(f'metadata_{self.node_id}.txt', 'w')
        for key, value in self.metadata.items():
            f.write(f'{key}:{value}\n')
        f.close()
        
    def load_state(self):
        if not os.path.exists(f'logs_{self.node_id}.txt'):
            f = open(f'logs_{self.node_id}.txt', 'w')
            f.close()
            return
        f = open(f'logs_{self.node_id}.txt', 'r')
        for line in f:
            self.logs.append(line)
        f.close()
        if not os.path.exists(f'metadata_{self.node_id}.txt'):
            f = open(f'metadata_{self.node_id}.txt', 'w')
            f.close()
            return
        f = open(f'metadata_{self.node_id}.txt', 'r')
        for line in f:
            key, value = line.split(':')
            self.metadata[key] = value
        f.close()
        
    def get(self, key):
        return self.metadata.get(key, None)