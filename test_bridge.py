import time
import fast_log_parser
from cluster_templates import TemplateClusterer
from anomaly_detector import ParameterAnomalyDetector # NEW: Import our ML module

print("Initializing Full AI Parsing Pipeline...")

# 1. Instantiate the Pipeline Components
parser = fast_log_parser.FastParser()
clusterer = TemplateClusterer(similarity_threshold=0.7)
anomaly_detector = ParameterAnomalyDetector(contamination=0.01) # 1% expected anomalies

log_file_path = "HDFS_2k.log"
line_count = 0
anomalies_found = 0

print(f"Reading {log_file_path} through the hybrid engine...\n")

start_time = time.time()

with open(log_file_path, "r") as file:
    for line in file:
        log_message = line.strip()
        if not log_message:
            continue
            
        # ---------------------------------------------------------
        # STEP 1: THE C++ FAST PATH (Parse & Mask)
        # ---------------------------------------------------------
        result = parser.parse_line(log_message)
        template_id = result['template_id']
        
        # ---------------------------------------------------------
        # STEP 2: ML CLUSTERING (Group similar structures)
        # ---------------------------------------------------------
        if result['is_new']:
            master_id, merged = clusterer.add_template(template_id, result['clean_log'])
        else:
            master_id = clusterer.cluster_map.get(template_id, template_id)

        # ---------------------------------------------------------
        # STEP 3: PARAMETER ANOMALY DETECTION
        # ---------------------------------------------------------
        # We pass the raw log so the detector can see the actual numbers!
        is_anomaly, msg = anomaly_detector.process_log(master_id, log_message)
        
        if is_anomaly:
            anomalies_found += 1
            print(f"  PARAMETER ANOMALY [{master_id}]: {msg}")
            print(f"   RAW: {log_message}\n")

        line_count += 1

end_time = time.time()
duration = end_time - start_time

print("-" * 50)
print(f"Processed {line_count} lines.")
print(f"Master Clusters Found: {clusterer.cluster_counter}")
print(f"Parameter Anomalies Detected: {anomalies_found}")
print(f"Time taken: {duration:.4f} seconds")
print(f"Throughput: {line_count / duration:,.0f} logs/sec")