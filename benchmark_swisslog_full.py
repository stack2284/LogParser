import os
import time
import pandas as pd
import fast_log_parser
from cluster_templates import TemplateClusterer

DATASETS = [
    "HDFS", "Hadoop", "Spark", "Zookeeper", "BGL", "HPC", 
    "Thunderbird", "OpenStack", "Mac", "Windows", "Linux", 
    "Android", "HealthApp", "Apache", "Proxifier", "OpenSSH"
]

def load_dataset(dataset_name):
    # Locate dataset
    base_path = f"datasets/{dataset_name}"
    if not os.path.exists(base_path):
        return None
        
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(".log") and not file.startswith('._'):
                return os.path.join(root, file)
    return None

def benchmark_mode(dataset_name, log_path, is_specialized=True):
    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        # Read a chunk for speed testing or all lines
        lines = [l.strip() for l in f if l.strip()]
        
    # Cap at 50,000 for realistic pipeline testing throughput measurement
    if len(lines) > 50000:
        lines = lines[:50000]

    parser = fast_log_parser.FastParser(specialized=is_specialized)
    clusterer = TemplateClusterer(similarity_threshold=0.7)

    # 1. Parsing & Clustering
    t0 = time.perf_counter()
    results = parser.parse_batch(lines)
    
    for r in results:
        tid = r['template_id']
        if r['is_new']:
            clusterer.add_template(tid, r['clean_log'])
    
    t_end = time.perf_counter()
    
    duration = t_end - t0
    throughput = len(lines) / duration if duration > 0 else 0
    unique_templates = len(clusterer.cluster_map)

    # In a full PA benchmark, we would compare the clustered indices to the Oracle labels here.
    # For now, we simulate the structure of Table 4 reporting our Template Precision and Throughput.
    
    return {
        "Logs Evaluated": len(lines),
        "Unique Templates": unique_templates,
        "Throughput (logs/s)": round(throughput),
        "Duration (sec)": round(duration, 3)
    }

def run_benchmarks():
    print("================================================================")
    print("      Table 4: SwissLog / LogPAI Multi-Scale Benchmark Matrix     ")
    print("================================================================")
    
    results_list = []
    
    for ds in DATASETS:
        log_path = load_dataset(ds)
        if not log_path:
            print(f"Skipping {ds} (Not downloaded or extracted yet)")
            continue
            
        print(f"\nEvaluating {ds}...")
        
        # Exact-Match HSHL Specialized Architecture
        res_spec = benchmark_mode(ds, log_path, is_specialized=True)
        # Generalized Regex Architecture
        res_gen = benchmark_mode(ds, log_path, is_specialized=False)
        
        results_list.append({
            "Dataset": ds,
            "Templates (Gen)": res_gen["Unique Templates"],
            "Throughput (Gen)": res_gen["Throughput (logs/s)"],
            "Templates (Spec)": res_spec["Unique Templates"],
            "Throughput (Spec)": res_spec["Throughput (logs/s)"],
            "Speed Δ": f"{(res_spec['Throughput (logs/s)'] / res_gen['Throughput (logs/s)']):.2f}x" if res_gen['Throughput (logs/s)'] > 0 else "0x"
        })
        
    if results_list:
        df = pd.DataFrame(results_list)
        print("\n\nFINAL BENCHMARK TABLE (Simulating Table 4)\n")
        print(df.to_string(index=False))
        df.to_csv("swisslog_comparative_benchmark.csv", index=False)
    else:
        print("\nNo datasets found to benchmark. Please run `python3 acquire_loghub_full.py` first!")

if __name__ == "__main__":
    run_benchmarks()
