"""
HSHL Log Parser — Full Optimized Pipeline with Observability

Performance optimizations (accuracy-safe):
- C++ batch parsing with OpenMP multi-threading (B1)
- Pre-extracted params & block_ids in C++ (B2+B5)
- Compiler: -O3 -march=native -flto -DNDEBUG
- IsolationForest with bounded deque + lazy numpy rebuild
- MinHash LSH at 128 permutations (accuracy-safe)
- Log-level pre-filtering for param anomaly only (B10)
- Prometheus metrics (B18)
"""
import time
import fast_log_parser
from cluster_templates import TemplateClusterer
from anomaly_detector import ParameterAnomalyDetector
from root_cause import RootCauseLocator

# Optional: Prometheus metrics (B18)
try:
    from prometheus_client import Histogram, Counter, Gauge, start_http_server
    
    PARSE_TIME = Histogram('hshl_parse_seconds', 'C++ batch parse time')
    CLUSTER_TIME = Histogram('hshl_cluster_seconds', 'Clustering time per batch')
    ANOMALY_TIME = Histogram('hshl_anomaly_seconds', 'Anomaly detection time per batch')
    LOGS_TOTAL = Counter('hshl_logs_processed_total', 'Total logs processed')
    ANOMALIES_TOTAL = Counter('hshl_anomalies_detected_total', 'Anomalies detected')
    TEMPLATES = Gauge('hshl_templates_total', 'Unique template clusters')
    THROUGHPUT = Gauge('hshl_throughput_logs_per_sec', 'Current throughput')
    
    HAS_METRICS = True
except ImportError:
    HAS_METRICS = False

def run_pipeline(log_file_path="HDFS_2k.log", enable_metrics_server=False):
    """Run the full HSHL pipeline on a log file.
    
    Args:
        log_file_path: Path to the log file
        enable_metrics_server: If True, start Prometheus HTTP server on port 8000
    """
    if HAS_METRICS and enable_metrics_server:
        start_http_server(8000)
        print("Prometheus metrics server started on :8000")

    # Initialize components
    parser = fast_log_parser.FastParser()
    clusterer = TemplateClusterer(similarity_threshold=0.7)  # 128 perms (default)
    param_detector = ParameterAnomalyDetector(contamination=0.01)
    locator = RootCauseLocator()

    start_time = time.time()

    # Read all lines
    with open(log_file_path, "r") as f:
        lines = [l.strip() for l in f if l.strip()]

    # C++ multi-threaded batch parse
    t0 = time.perf_counter()
    results = parser.parse_batch(lines)
    parse_time = time.perf_counter() - t0
    if HAS_METRICS: PARSE_TIME.observe(parse_time)

    # Cache method lookups for tight loop
    _cluster_add = clusterer.add_template
    _cluster_map = clusterer.cluster_map
    _detect = param_detector.process_log
    _locate = locator.add_log

    line_count = 0
    anomalies = 0

    for res in results:
        tid = res['template_id']
        
        # Clustering (always runs — never skipped)
        if res['is_new']:
            mid, _ = _cluster_add(tid, res['clean_log'])
        else:
            mid = _cluster_map.get(tid, tid)
        
        # Parameter Anomaly Detection
        # B10: skip ONLY param anomaly for DEBUG/TRACE (sequence context preserved)
        if not res['skip_anomaly']:
            params = res['params']
            if params:
                is_anomaly, msg = _detect(mid, params)
                if is_anomaly:
                    anomalies += 1
                    if HAS_METRICS: ANOMALIES_TOTAL.inc()
        
        # Root cause (always runs — uses pre-extracted block_id from C++)
        block_id = res['block_id']
        if block_id:
            _locate(mid, block_id, line_count)

        line_count += 1

    duration = time.time() - start_time

    if HAS_METRICS:
        LOGS_TOTAL.inc(line_count)
        TEMPLATES.set(clusterer.cluster_counter)
        THROUGHPUT.set(line_count / duration)

    # Report
    throughput = line_count / duration
    print(f"Processed {line_count} lines in {duration:.4f}s")
    print(f"Throughput: {throughput:,.0f} logs/sec")
    print(f"Templates: {clusterer.cluster_counter} | Anomalies: {anomalies}")
    
    return {
        "lines": line_count,
        "duration": duration,
        "throughput": throughput,
        "templates": clusterer.cluster_counter,
        "anomalies": anomalies,
    }


if __name__ == "__main__":
    run_pipeline()