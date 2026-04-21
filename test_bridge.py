import time
import fast_log_parser
from cluster_templates import TemplateClusterer
from anomaly_detector import ParameterAnomalyDetector

print("Initializing Full AI Parsing Pipeline...")

# 1. Instantiate the Pipeline Components
parser = fast_log_parser.FastParser()
clusterer = TemplateClusterer(similarity_threshold=0.7)  # 128 perms (default)
anomaly_detector = ParameterAnomalyDetector(contamination=0.01)

log_file_path = "HDFS_2k.log"
anomalies_found = 0

print(f"Reading {log_file_path} through the hybrid engine...\n")

start_time = time.time()

# Read and batch parse
with open(log_file_path, "r") as f:
    lines = [l.strip() for l in f if l.strip()]

results = parser.parse_batch(lines)

line_count = 0
for res in results:
    template_id = res['template_id']
    
    # STEP 2: ML CLUSTERING
    if res['is_new']:
        master_id, merged = clusterer.add_template(template_id, res['clean_log'])
    else:
        master_id = clusterer.cluster_map.get(template_id, template_id)

    # STEP 3: PARAMETER ANOMALY DETECTION
    # B10: skip anomaly detection for DEBUG/TRACE only
    if not res.get('skip_anomaly', False):
        params = res['params']
        if params:
            is_anomaly, msg = anomaly_detector.process_log(master_id, params)
            
            if is_anomaly:
                anomalies_found += 1
                print(f"  PARAMETER ANOMALY [{master_id}]: {msg}")
                print(f"   RAW: {lines[line_count]}\n")

    line_count += 1

end_time = time.time()
duration = end_time - start_time

print("-" * 50)
print(f"Processed {line_count} lines.")
print(f"Master Clusters Found: {clusterer.cluster_counter}")
print(f"Parameter Anomalies Detected: {anomalies_found}")
print(f"Time taken: {duration:.4f} seconds")
print(f"Throughput: {line_count / duration:,.0f} logs/sec")