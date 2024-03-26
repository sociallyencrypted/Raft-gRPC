import os

class Storage:
    def __init__(self, node_id):
        self.node_id = node_id
        self.folder_location = f'logs_node_{self.node_id}'
        self.logs = []
        self.metadata = {}
        self.state = {}
        self.load_state()
        
    def write_to_dump(self, statement):
        f = open(f'{self.folder_location}/dump.txt', 'a')
        f.write(statement)
        f.close()

    def append_log(self, type, term, key=None, value=None):
        if type == 'NO-OP':
            entry = f'NO-OP {term}\n'
        else:
            entry = f'SET {key} {value} {term}\n'
            self.state[key] = value
        self.logs.append(entry)
        f = open(f'{self.folder_location}/logs.txt', 'a')
        f.write(entry)
        f.close()
        
    def load_state(self):
        if not os.path.exists(f'{self.folder_location}/logs.txt'):
            f = open(f'{self.folder_location}/logs.txt', 'w')
            f.close()
            return
        f = open(f'{self.folder_location}/logs.txt', 'r')
        for line in f:
            self.logs.append(line)
        f.close()
        if not os.path.exists(f'{self.folder_location}/metadata.txt'):
            f = open(f'{self.folder_location}/metadata.txt', 'w')
            f.close()
            return
        f = open(f'{self.folder_location}/metadata.txt', 'r')
        for line in f:
            key, value = line.split(':')
            self.metadata[key] = value
        f.close()
        
    def get(self, key):
        if key in self.state:
            return self.state[key]
        else:
            return None