import os
import urllib.request
import pandas as pd
import time
from collections import defaultdict
import fast_log_parser
from cluster_templates import TemplateClusterer

TARGETS = ["HDFS", "Hadoop", "Zookeeper", "Spark", "Mac", "Linux", "Apache"]
BASE_URL = "https://raw.githubusercontent.com/logpai/loghub/master/"

os.makedirs("oracle_data", exist_ok=True)

def download_loghub_2k(dataset):
    log_url = f"{BASE_URL}{dataset}/{dataset}_2k.log"
    csv_url = f"{BASE_URL}{dataset}/{dataset}_2k.log_structured.csv"
    
    log_path = f"oracle_data/{dataset}_2k.log"
    csv_path = f"oracle_data/{dataset}_2k.log_structured.csv"
    
    for url, path in [(log_url, log_path), (csv_url, csv_path)]:
        if not os.path.exists(path):
            print(f"Downloading {url} to {path}...")
            urllib.request.urlretrieve(url, path)
            
    return log_path, csv_path

def evaluate_PA(log_path, csv_path):
    # 1. Provide Ground Truth groupings
    df_ground = pd.read_csv(csv_path)
    # df_ground has 'LineId', 'EventId', 'EventTemplate'
    
    oracle_groups = {}
    for i, row in df_ground.iterrows():
        eid = row['EventId']
        if eid not in oracle_groups:
            oracle_groups[eid] = set()
        oracle_groups[eid].add(i)
        
    oracle_line_to_group = {}
    for eid, s in oracle_groups.items():
        for line_idx in s:
            oracle_line_to_group[line_idx] = eid

    # 2. Run our HSHL Generalized Parser
    with open(log_path, 'r', encoding='utf-8') as f:
        lines = [l.strip() for l in f if l.strip()]
        
    parser = fast_log_parser.FastParser(specialized=False) # Generalized for max accuracy!
    
    results = parser.parse_batch(lines)
    
    predicted_groups = defaultdict(set)
    clean_logs = []
    for i, res in enumerate(results):
        tid = res['template_id']
        clean_logs.append(res['clean_log'])
        # Group purely by the deterministic C++ Template IDs, bypassing lossy MinHash merging!
        predicted_groups[tid].add(i)

    # 3. Calculate Parsing Accuracy Metric
    # PA = exactly identical clustered pairs / total lines
    accurate_events = 0
    pa_miss_count = 0
    
    for mid, s in predicted_groups.items():
        # A predicted cluster is accurate ONLY if it matches the oracle cluster exactly 1:1
        # Pick the oracle class of the first item in our cluster
        rep_idx = next(iter(s))
        if rep_idx not in oracle_line_to_group:
            continue
            
        oracle_class = oracle_line_to_group[rep_idx]
        oracle_expected_set = oracle_groups[oracle_class]
        
        if s == oracle_expected_set:
            accurate_events += len(s)
            
    pa = accurate_events / len(lines)
    return pa

if __name__ == "__main__":
    print("========== LOGPAI EXACT PA EVALUATION ==========")
    for ds in TARGETS:
        try:
            log_p, csv_p = download_loghub_2k(ds)
            pa_score = evaluate_PA(log_p, csv_p)
            print(f"[*] {ds:<10} | True PA = {pa_score:.4f} ({(pa_score * 100):.2f}%)")
        except Exception as e:
            print(f"[-] {ds:<10} | Error: {e}")
