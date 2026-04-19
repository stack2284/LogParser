import time
import re
import fast_log_parser
from cluster_templates import TemplateClusterer
from anomaly_detector import ParameterAnomalyDetector
from root_cause import RootCauseLocator

parser = fast_log_parser.FastParser()
clusterer = TemplateClusterer(similarity_threshold=0.7)
param_detector = ParameterAnomalyDetector(contamination=0.01)
locator = RootCauseLocator()

log_file_path = "HDFS_2k.log"
line_count = 0

start_time = time.time()

with open(log_file_path, "r") as file:
    for line in file:
        log = line.strip()
        if not log: continue
            
        res = parser.parse_line(log)
        tid = res['template_id']
        
        if res['is_new']:
            mid, _ = clusterer.add_template(tid, res['clean_log'])
        else:
            mid = clusterer.cluster_map.get(tid, tid)
            
        param_anom, _ = param_detector.process_log(mid, log)
        
        blk_match = re.search(r'(blk_-?\d+)', log)
        if blk_match:
            locator.add_log(mid, blk_match.group(1), line_count)

        line_count += 1

duration = time.time() - start_time

print(f"Processed {line_count} lines.")
print(f"Time: {duration:.4f}s")
print("Full pipeline integrated.")