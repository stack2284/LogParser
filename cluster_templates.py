import re
from datasketch import MinHash, MinHashLSH

class TemplateClusterer:
    def __init__(self, similarity_threshold=0.7, num_perm=128):
        # We use a 70% similarity threshold to decide if templates should merge 
        self.threshold = similarity_threshold
        self.num_perm = num_perm
        
        # Initialize LSH to avoid pairwise comparisons [cite: 114-115]
        # Reduced from 128 to 32 permutations — sufficient for log template similarity
        # and 4x faster MinHash creation
        self.lsh = MinHashLSH(threshold=self.threshold, num_perm=self.num_perm)
        
        # Keep track of the actual text for each template ID
        self.template_store = {}
        
        # Maps a raw C++ Template ID (T0001) to a Clustered Master ID (C0001)
        self.cluster_map = {}
        self.cluster_counter = 0
        
        # Precompiled regex for token extraction
        self._token_re = re.compile(r'[a-zA-Z]+')

    def _get_tokens(self, text):
        # Extract just the structural words, ignoring the <IP>, <NUM> tags
        return set(self._token_re.findall(text))

    def add_template(self, template_id, clean_log):
        # If we already clustered this, return its master cluster ID
        if template_id in self.cluster_map:
            return self.cluster_map[template_id], True

        tokens = self._get_tokens(clean_log)
        if not tokens:
            return template_id, False

        # Create a MinHash signature for this template 
        m = MinHash(num_perm=self.num_perm)
        for token in tokens:
            m.update(token.encode('utf8'))

        # Query the LSH index in O(1) time to find similar templates
        result = self.lsh.query(m)

        if result:
            # We found similar templates! Merge it into the first match 
            master_id = self.cluster_map[result[0]]
            self.cluster_map[template_id] = master_id
            return master_id, True # True means it was merged
        else:
            # No similar templates found. Create a new Master Cluster [cite: 111]
            self.cluster_counter += 1
            master_id = f"C{self.cluster_counter:04d}"
            
            self.cluster_map[template_id] = master_id
            self.template_store[template_id] = clean_log
            
            # Insert this new signature into the LSH index so future logs can match it
            self.lsh.insert(template_id, m)
            return master_id, False # False means it is a brand new cluster

# Quick test of the logic
if __name__ == "__main__":
    clusterer = TemplateClusterer()
    
    # Let's simulate two templates that are slightly different
    t1_text = "INFO dfs.DataNode$PacketResponder: PacketResponder <NUM> for block <ID> terminating"
    t2_text = "INFO dfs.DataNode$PacketResponder: PacketResponder <NUM> for block <ID> terminating abruptly"
    
    print("Testing Clustering Logic...")
    c1, merged1 = clusterer.add_template("T0001", t1_text)
    print(f"Template T0001 assigned to Cluster: {c1} (Merged: {merged1})")
    
    c2, merged2 = clusterer.add_template("T0002", t2_text)
    print(f"Template T0002 assigned to Cluster: {c2} (Merged: {merged2})")