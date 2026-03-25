import time
from kafka import KafkaProducer

# Connect to the local Kafka broker
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# The name of the file we just downloaded
log_file_path = "HDFS_2k.log"
print(f"Reading real logs from {log_file_path}... Press Ctrl+C to stop.")

try:
    # Open the real log file
    with open(log_file_path, "r") as file:
        for line in file:
            # Clean up any extra spaces or newlines at the end of the log
            log_message = line.strip()
            
            if not log_message:
                continue
                
            # Send the real log line to our Kafka topic 'raw-logs'
            producer.send('raw-logs', value=log_message.encode('utf-8'))
            print(f"Sent: {log_message}")
            
            # Pause for half a second so we can watch it stream
            time.sleep(0.5)
            
except FileNotFoundError:
    print(f"Error: Could not find {log_file_path}. Did you download it?")
except KeyboardInterrupt:
    print("\nStopping generator.")
finally:
    # Always close the connection cleanly
    producer.close()