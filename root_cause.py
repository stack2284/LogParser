import networkx as nx

class RootCauseLocator:
    def __init__(self):
        self.id_graph = nx.DiGraph()
        self.log_groups = {}

    def add_log(self, cluster_id, block_id, timestamp):
        if block_id not in self.log_groups:
            self.log_groups[block_id] = []
        self.log_groups[block_id].append((timestamp, cluster_id))
        
    def trace_anomaly(self, block_id):
        if block_id in self.log_groups:
            return sorted(self.log_groups[block_id])
        return []

locator = RootCauseLocator()
locator.add_log("C0001", "blk_123", 1)
locator.add_log("C0002", "blk_123", 2)
locator.add_log("C0009", "blk_123", 3) 

print(locator.trace_anomaly("blk_123"))