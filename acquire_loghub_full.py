import os
import requests
import tarfile
import zipfile
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

DATASETS = {
    "HDFS": "https://zenodo.org/record/3227177/files/HDFS_1.tar.gz",
    "Hadoop": "https://zenodo.org/record/3227177/files/Hadoop.tar.gz",
    "Spark": "https://zenodo.org/record/3227177/files/Spark.tar.gz",
    "Zookeeper": "https://zenodo.org/record/3227177/files/Zookeeper.tar.gz",
    "BGL": "https://zenodo.org/record/3227177/files/BGL.tar.gz",
    "HPC": "https://zenodo.org/record/3227177/files/HPC.tar.gz",
    "Thunderbird": "https://zenodo.org/record/3227177/files/Thunderbird.tar.gz",
    "OpenStack": "https://zenodo.org/record/3227177/files/OpenStack.tar.gz",
    "Mac": "https://zenodo.org/record/3227177/files/Mac.tar.gz",
    "Windows": "https://zenodo.org/record/3227177/files/Windows.tar.gz",
    "Linux": "https://zenodo.org/record/3227177/files/Linux.tar.gz",
    "Android": "https://zenodo.org/record/3227177/files/Android.tar.gz",
    "HealthApp": "https://zenodo.org/record/3227177/files/HealthApp.tar.gz",
    "Apache": "https://zenodo.org/record/3227177/files/Apache.tar.gz",
    "Proxifier": "https://zenodo.org/record/3227177/files/Proxifier.tar.gz",
    "OpenSSH": "https://zenodo.org/record/3227177/files/OpenSSH.tar.gz"
}

os.makedirs("datasets", exist_ok=True)

def download_and_extract(name, url, position):
    dest_path = f"datasets/{name}.tar.gz"
    
    # 1. Download
    if not os.path.exists(dest_path):
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            total_size_in_bytes = int(response.headers.get('content-length', 0))
            block_size = 1048576 # 1MB
            
            # Position the progress bar based on the thread index so they stack nicely
            progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True, desc=f"Downloading {name}", position=position, leave=True)
            with open(dest_path, 'wb') as file:
                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    file.write(data)
            progress_bar.close()
            
        except Exception as e:
            return f"❌ Failed to download {name}: {e}"
    else:
        # Just to keep visual consistency
        tqdm.write(f"✅ Archive found for {name}, skipped download.")

    # 2. Extract
    extract_target = f"datasets/{name}"
    if not os.path.exists(extract_target):
        tqdm.write(f"⏳ Extracting {name}...")
        try:
            if dest_path.endswith("tar.gz"):
                with tarfile.open(dest_path, "r:gz") as tar:
                    tar.extractall(path=extract_target)
            return f"✅ Done downloading and extracting {name}"
        except Exception as e:
            return f"❌ Failed to extract {name}: {e}"
    return f"✅ {name} already extracted."
            
if __name__ == '__main__':
    print("Initiating PARALLEL LogHub/SwissLog dataset synchronizer (77GB Total)")
    print("This will download 4 datasets concurrently to saturate your 300+ Mbps connection!")
    
    # We use max_workers=4 so we don't completely crash the router or get IP blocked by Zenodo
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for i, (name, url) in enumerate(DATASETS.items()):
            futures.append(executor.submit(download_and_extract, name, url, i))
            
        for future in as_completed(futures):
            tqdm.write(future.result())
    
    print("\n\nAll downloads complete! You are ready to run benchmark_swisslog_full.py")
