import paramiko
import re
import csv
from datetime import datetime

# === CONFIG ===
GPU_NODES = [f"r04gn0{i}" for i in range(1, 7)] + [f"r05gn0{i}" for i in range(1, 7)]
SSH_USER = "your_user"  # <-- Replace with real username
CSV_FILE = "gpu_card_status_log.csv"
COMMAND = "/usr/bin/nvidia-smi"  # Or just "nvidia-smi" if in PATH

# === HELPERS ===
def ssh_nvidia_smi(host):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=SSH_USER)
    stdin, stdout, stderr = client.exec_command(COMMAND)
    output = stdout.read().decode()
    client.close()
    return output

def format_card_id(node_id, gpu_index):
    row = node_id[1:3]
    col = node_id[-2:]
    gpu = f"{int(gpu_index)+1:02d}"
    return f"{row}_{col}_{gpu}"

def parse_nvidia_output(output, node_id):
    timestamp = datetime.now().replace(microsecond=0)
    results = []

    # Extract GPU info using regex
    gpu_matches = re.findall(
        r"\|\s+(\d+)\s+NVIDIA.*?\|\s*\n\|\s+N/A\s+(\d+)C\s+\S+\s+(\d+)W\s*/\s*\d+W\s+\|\s+([\d]+)MiB\s*/\s+[\d]+MiB\s+\|\s+(\d+)%", 
        output, 
        re.DOTALL
    )

    found_gpus = set()
    for gpu_id, temp, power, mem, util in gpu_matches:
        card_id = format_card_id(node_id, gpu_id)
        found_gpus.add(int(gpu_id))
        results.append([
            card_id,
            timestamp,
            True,  # is_healthy
            float(power),
            float(util),
            float(mem) / 1024,
            float(temp)
        ])

    # Fill missing GPUs
    for i in range(4):
        if i not in found_gpus:
            card_id = format_card_id(node_id, i)
            results.append([card_id, timestamp, False, None, None, None, None])

    return results

# === MAIN ===
all_data = []

for node in GPU_NODES:
    try:
        output = ssh_nvidia_smi(node)
        all_data.extend(parse_nvidia_output(output, node))
    except Exception as e:
        print(f"❌ Error with {node}: {e}")

# === WRITE CSV ===
if all_data:
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(all_data)
    print(f"✅ Wrote {len(all_data)} GPU card records to {CSV_FILE}")
else:
    print("⚠️ No GPU data collected.")
