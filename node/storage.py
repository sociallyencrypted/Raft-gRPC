class Storage:
    def __init__(self, node_id):
        self.node_id = node_id
        self.logs = []
        self.metadata = {}
        # Load logs and metadata from disk

    def append_log(self, entry):
        # Append log entry to logs

    def dump_state(self):
        # Dump logs, metadata, and other state to disk