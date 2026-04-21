"""
HSHL Log Parser — Comprehensive Benchmark Suite

Tests the optimized pipeline at different scales and compares against baseline.
Also validates correctness (template count, anomaly detection).
"""
import time
import os
import fast_log_parser
from cluster_templates import TemplateClusterer
from anomaly_detector import ParameterAnomalyDetector
from root_cause import RootCauseLocator

LOG_FILE = "HDFS_2k.log"
BASELINE_TIME = 6.169   # seconds (measured before optimization)
BASELINE_THROUGHPUT = 324  # logs/sec

def load_lines():
    with open(LOG_FILE, "r") as f:
        return [l.strip() for l in f if l.strip()]

def run_full_pipeline(lines):
    """Run full pipeline and return timing + results."""
    parser = fast_log_parser.FastParser()
    clusterer = TemplateClusterer(similarity_threshold=0.7)
    detector = ParameterAnomalyDetector(contamination=0.01)
    locator = RootCauseLocator()

    t0 = time.perf_counter()
    results = parser.parse_batch(lines)

    _ca = clusterer.add_template
    _cm = clusterer.cluster_map
    _det = detector.process_log
    _loc = locator.add_log

    anomalies = 0
    for i, res in enumerate(results):
        tid = res['template_id']
        if res['is_new']:
            mid, _ = _ca(tid, res['clean_log'])
        else:
            mid = _cm.get(tid, tid)
        
        if not res['skip_anomaly']:
            params = res['params']
            if params:
                is_anom, _ = _det(mid, params)
                if is_anom:
                    anomalies += 1
        
        bid = res['block_id']
        if bid:
            _loc(mid, bid, i)

    elapsed = time.perf_counter() - t0
    return elapsed, len(lines), clusterer.cluster_counter, anomalies

def benchmark_scale(lines, multiplier, runs=5):
    """Benchmark at a given scale (multiply lines N times)."""
    scaled = lines * multiplier
    times = []
    for _ in range(runs):
        t, n, templates, anomalies = run_full_pipeline(scaled)
        times.append(t)
    avg = sum(times) / len(times)
    best = min(times)
    return {
        "lines": len(scaled),
        "avg_ms": avg * 1000,
        "best_ms": best * 1000,
        "avg_throughput": len(scaled) / avg,
        "best_throughput": len(scaled) / best,
        "templates": templates,
    }

def main():
    print("=" * 70)
    print("  HSHL Log Parser — Benchmark Suite")
    print("=" * 70)
    print()
    
    lines = load_lines()
    print(f"Base dataset: {len(lines)} lines from {LOG_FILE}")
    print()

    # Test at multiple scales
    scales = [1, 5, 10, 50]
    
    print(f"{'Scale':<10} {'Lines':<10} {'Avg ms':<12} {'Best ms':<12} {'Avg logs/s':<15} {'Best logs/s':<15} {'Templates':<10}")
    print("-" * 84)
    
    for mult in scales:
        r = benchmark_scale(lines, mult, runs=5)
        print(f"{mult}x{'':<8} {r['lines']:<10} {r['avg_ms']:<12.2f} {r['best_ms']:<12.2f} {r['avg_throughput']:<15,.0f} {r['best_throughput']:<15,.0f} {r['templates']:<10}")
    
    print()
    
    # Correctness check
    print("--- Correctness Verification ---")
    t, n, templates, anomalies = run_full_pipeline(lines)
    print(f"  Lines processed:     {n}")
    print(f"  Templates found:     {templates}")
    print(f"  Expected templates:  13")
    print(f"  Match:               {'✅ PASS' if templates == 13 else '❌ FAIL'}")
    print()
    
    # Speedup summary
    avg_1x = benchmark_scale(lines, 1, runs=10)
    speedup = BASELINE_TIME / (avg_1x['avg_ms'] / 1000)
    print("--- Final Summary ---")
    print(f"  Baseline:     {BASELINE_TIME:.3f}s / {BASELINE_THROUGHPUT} logs/sec")
    print(f"  Optimized:    {avg_1x['avg_ms']:.2f}ms / {avg_1x['avg_throughput']:,.0f} logs/sec")
    print(f"  Speedup:      {speedup:.0f}x faster")
    print(f"  Improvement:  {(1 - avg_1x['avg_ms']/1000/BASELINE_TIME) * 100:.1f}% time reduction")
    print()

if __name__ == "__main__":
    main()