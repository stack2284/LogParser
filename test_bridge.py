import time

# 1. Import your custom compiled C++ module!
import fast_log_parser

print("Successfully imported C++ FastParser module!")

# 2. Instantiate the C++ class
parser = fast_log_parser.FastParser()

log_file_path = "HDFS_2k.log"
line_count = 0

print(f"Reading {log_file_path} through the C++ engine...\n")

start_time = time.time()

with open(log_file_path, "r") as file:
    for line in file:
        log_message = line.strip()
        if not log_message:
            continue
            
        # 3. Call the C++ function natively from Python
        # It returns a standard Python dictionary!
        result = parser.parse_line(log_message)
        
        line_count += 1
        
        # Print the first 5 results to verify the bridge works
        if line_count <= 5:
            if result['is_new']:
                print(f"🌟 NEW: [{result['template_id']}] {result['clean_log']}")
            else:
                print(f"🔄 OLD: [{result['template_id']}] {result['clean_log']}")

end_time = time.time()
duration = end_time - start_time

print("-" * 50)
print(f"Processed {line_count} lines from Python.")
print(f"Time taken: {duration:.4f} seconds")
print(f"Throughput: {line_count / duration:,.0f} logs/sec")