import re
import hashlib
import redis
from kafka import KafkaConsumer

# ---------------------------------------------------------
# SETUP
# ---------------------------------------------------------

# Connect to the local Kafka broker
consumer = KafkaConsumer(
    'raw-logs',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: x.decode('utf-8')
)

# Connect to the local Redis database [cite: 100]
# decode_responses=True ensures we get normal strings back, not byte arrays
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Step 2: Preprocessing Regex Patterns
REGEX_IP = re.compile(r'\d+\.\d+\.\d+\.\d+')
REGEX_ID = re.compile(r'[a-zA-Z]+_-?\d+') 
REGEX_NUM = re.compile(r'\b\d+\b')
REGEX_PATH = re.compile(r'\/[^\s]+') 


# ---------------------------------------------------------
# PIPELINE FUNCTIONS
# ---------------------------------------------------------

def preprocess_log(log_message):
    log_message = REGEX_IP.sub('<IP>', log_message)
    log_message = REGEX_ID.sub('<ID>', log_message)
    log_message = REGEX_NUM.sub('<NUM>', log_message)
    log_message = REGEX_PATH.sub('<PATH>', log_message) 
    return log_message

def tokenize_log(clean_log):
    tokens = re.split(r'[\s:,=]+', clean_log)
    return [token for token in tokens if token]

# Step 4: Template Hashing Module
def identify_template(tokens):
    # 1. Extract constant tokens (ignore anything wrapped in < >) [cite: 97]
    constants = [token for token in tokens if not (token.startswith('<') and token.endswith('>'))]
    
    # Create a single string out of the constants to hash
    constant_string = " ".join(constants)
    
    # 2. Generate a hash signature using MD5 [cite: 98]
    signature = hashlib.md5(constant_string.encode('utf-8')).hexdigest()
    
    # 3. Fast template identification using Redis [cite: 95, 100]
    # We check if this signature already exists in Redis
    template_id = redis_client.get(signature)
    
    # If it doesn't exist, this is a brand new template!
    if not template_id:
        # Ask Redis to increment our counter to get a new ID number (1, 2, 3...)
        new_id_num = redis_client.incr('template_counter')
        
        # Format it nicely (e.g., T0001, T0002)
        template_id = f"T{new_id_num:04d}"
        
        # Save the mapping in Redis: Hash -> Template ID [cite: 102]
        redis_client.set(signature, template_id)
        is_new = True
    else:
        is_new = False
        
    return template_id, constant_string, is_new

# ---------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------

print("System running! Waiting for logs...")
print("Connecting to Kafka and Redis...\n")

try:
    for message in consumer:
        raw_log = message.value
        
        # Run the pipeline
        clean_log = preprocess_log(raw_log)
        tokenized_log = tokenize_log(clean_log)
        template_id, constant_string, is_new = identify_template(tokenized_log)
        
        # Print the results
        print("-" * 50)
        if is_new:
            print(f"  NEW TEMPLATE DISCOVERED: {template_id}")
        else:
            print(f"  MATCHED EXISTING TEMPLATE: {template_id}")
            
        print(f"RAW:       {raw_log}")
        print(f"CONSTANTS: {constant_string}")
        
except KeyboardInterrupt:
    print("\nStopping consumer.")
finally:
    consumer.close()