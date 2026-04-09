import re
import numpy as np
from sklearn.ensemble import IsolationForest

class ParameterAnomalyDetector:
    def __init__(self, contamination=0.01):
        # contamination = 0.01 means we expect roughly 1% of logs to be anomalies
        self.contamination = contamination
        
        # We maintain a separate model and data history for EACH Master Cluster (C0001, C0002, etc.)
        self.models = {}
        self.history = {}
        self.expected_lengths = {}
        
        # Minimum number of logs we need to see before we start predicting
        self.warmup_period = 50 

    def _extract_numbers(self, raw_log):
        # Extract all standalone numbers from the raw string
        numbers = re.findall(r'\b\d+\b', raw_log)
        return [float(num) for num in numbers]
    def process_log(self, master_cluster_id, raw_log):
        numbers = self._extract_numbers(raw_log)
        
        # If this log has no numbers, it can't have parameter anomalies
        if not numbers:
            return False, "No parameters"

        # Initialize storage for new clusters
        if master_cluster_id not in self.models:
            self.models[master_cluster_id] = IsolationForest(contamination=self.contamination, random_state=42)
            self.history[master_cluster_id] = []
            
            # NEW: Record the exact number of features this cluster expects
            self.expected_lengths[master_cluster_id] = len(numbers)

        # ---------------------------------------------------------
        # NEW: FEATURE ALIGNMENT (Pad or Truncate)
        # ---------------------------------------------------------
        expected_len = self.expected_lengths[master_cluster_id]
        
        if len(numbers) > expected_len:
            # Truncate extra numbers
            numbers = numbers[:expected_len]
        elif len(numbers) < expected_len:
            # Pad missing numbers with zeros
            numbers = numbers + [0.0] * (expected_len - len(numbers))

        # Save the numerical data
        self.history[master_cluster_id].append(numbers)
        current_data = np.array(self.history[master_cluster_id])

        # ---------------------------------------------------------
        # THE WARMUP PHASE (Training)
        # ---------------------------------------------------------
        if len(self.history[master_cluster_id]) < self.warmup_period:
            return False, f"Warming up ({len(self.history[master_cluster_id])}/{self.warmup_period})"
        
        if len(self.history[master_cluster_id]) == self.warmup_period:
            self.models[master_cluster_id].fit(current_data)
            return False, "Model Trained!"

        # ---------------------------------------------------------
        # THE DETECTION PHASE (Predicting)
        # ---------------------------------------------------------
        if len(self.history[master_cluster_id]) % 100 == 0:
            self.models[master_cluster_id].fit(current_data)

        # Predict! Isolation Forest returns 1 for normal, -1 for anomaly
        prediction = self.models[master_cluster_id].predict([numbers])[0]
        
        is_anomaly = (prediction == -1)
        return is_anomaly, f"Params: {numbers}"
    

# Quick local test
if __name__ == "__main__":
    detector = ParameterAnomalyDetector(contamination=0.1)
    
    # Simulate 50 normal logs for Cluster C0001
    print("Sending normal logs to warm up the model...")
    for i in range(50):
        detector.process_log("C0001", f"Received block blk_123 of size 67108864 from 192.168.1.{i}")
    
    # Simulate an anomaly (Massive file size)
    print("\nSending Anomaly (Massive File Size)...")
    is_anomaly, msg = detector.process_log("C0001", "Received block blk_123 of size 999999999999 from 192.168.1.50")
    print(f"Anomaly Detected: {is_anomaly} | {msg}")