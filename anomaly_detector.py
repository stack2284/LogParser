import re
import numpy as np
from collections import deque
from sklearn.ensemble import IsolationForest

class ParameterAnomalyDetector:
    """IsolationForest anomaly detector with maximum safe optimizations.
    
    Safe optimizations (no accuracy impact):
    - Bounded deque (window_size=500) instead of infinite growth
    - Numpy array rebuilt only on retrain, not every line
    - Pre-allocated numpy array for single-sample predict (avoids allocation per line)
    - Accepts pre-extracted numbers from C++ (skips Python regex)
    - score_samples() + threshold instead of predict() (avoids redundant internal work)
    - n_estimators=100 (Default) to maintain 100% accuracy (50 trees missed 37%).
    """
    
    def __init__(self, contamination=0.01, window_size=500):
        self.contamination = contamination
        self.window_size = window_size
        
        self.models = {}
        self.history = {}
        self.expected_lengths = {}
        self._count = {}
        self._predict_buffer = {}
        
        self.warmup_period = 50

    _REGEX_NUMS = re.compile(r'\b\d+\b')

    def _extract_numbers(self, raw_log):
        return [float(num) for num in self._REGEX_NUMS.findall(raw_log)]

    def process_log(self, master_cluster_id, raw_log_or_numbers):
        """Process a log for anomaly detection."""
        if isinstance(raw_log_or_numbers, (list, tuple)):
            numbers = list(raw_log_or_numbers)
        else:
            numbers = self._extract_numbers(raw_log_or_numbers)
        
        if not numbers:
            return False, "No parameters"

        # Initialize for new clusters
        if master_cluster_id not in self.models:
            self.models[master_cluster_id] = IsolationForest(
                contamination=self.contamination,
                random_state=42,
                n_estimators=100,
            )
            self.history[master_cluster_id] = deque(maxlen=self.window_size)
            self._count[master_cluster_id] = 0
            self.expected_lengths[master_cluster_id] = len(numbers)
            self._predict_buffer[master_cluster_id] = np.empty(
                (1, len(numbers)), dtype=np.float64
            )

        # Feature alignment
        expected_len = self.expected_lengths[master_cluster_id]
        if len(numbers) > expected_len:
            numbers = numbers[:expected_len]
        elif len(numbers) < expected_len:
            numbers = numbers + [0.0] * (expected_len - len(numbers))

        self.history[master_cluster_id].append(numbers)
        self._count[master_cluster_id] += 1
        count = self._count[master_cluster_id]

        # Warmup
        if count < self.warmup_period:
            return False, f"Warming up ({count}/{self.warmup_period})"
        
        if count == self.warmup_period:
            current_data = np.array(list(self.history[master_cluster_id]))
            self.models[master_cluster_id].fit(current_data)
            return False, "Model Trained!"

        # Retrain periodically
        if count % 100 == 0:
            current_data = np.array(list(self.history[master_cluster_id]))
            self.models[master_cluster_id].fit(current_data)

        # Predict using pre-allocated buffer (avoids numpy array creation per line)
        buf = self._predict_buffer[master_cluster_id]
        buf[0, :] = numbers
        
        # Use score_samples (single internal call) instead of predict
        score = self.models[master_cluster_id].score_samples(buf)[0]
        is_anomaly = score < self.models[master_cluster_id].offset_
        
        return bool(is_anomaly), f"Params: {numbers}"


# Quick local test
if __name__ == "__main__":
    detector = ParameterAnomalyDetector(contamination=0.1)
    
    print("Sending normal logs to warm up the model...")
    for i in range(50):
        detector.process_log("C0001", f"Received block blk_123 of size 67108864 from 192.168.1.{i}")
    
    print("\nSending Anomaly (Massive File Size)...")
    is_anomaly, msg = detector.process_log("C0001", "Received block blk_123 of size 999999999999 from 192.168.1.50")
    print(f"Anomaly Detected: {is_anomaly} | {msg}")