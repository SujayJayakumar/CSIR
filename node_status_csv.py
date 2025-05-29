import csv
import datetime
import paramiko

NODES_FILE = "nodes.txt"
CSV_FILE = "node_status_test.csv"
SSH_HOST = "swapnil@192.168.103.50"
SSH_COMMAND = "/opt/pbs/bin/pbsnodes -l"

# Hardcoded SSH password for testing only 
SSH_PASSWORD = "Sachinbhai@3"


def get_all_nodes():
    with open(NODES_FILE, "r") as f:
        return [line.strip() for line in f if line.strip()]


def run_remote_command():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    user, host = SSH_HOST.split("@")
    client.connect(
        hostname=host,
        username=user,
        password=SSH_PASSWORD,
        timeout=10,
        look_for_keys=False,
        allow_agent=False
    )
    stdin, stdout, stderr = client.exec_command(SSH_COMMAND)
    output = stdout.read().decode()
    client.close()
    return output


def parse_output(output):
    status_map = {}
    for line in output.strip().splitlines():
        if not line.strip():
            continue

        parts = line.split(maxsplit=1)
        node_id = parts[0]
        status_text = parts[1].strip() if len(parts) > 1 else ""

        # Get only the comma-separated flags (ignore the reason part)
        raw_flags = status_text.split(" ", 1)[0]
        status_flags = [flag.strip().lower() for flag in raw_flags.split(",")]

        # Prioritize status down > offline
        if "down" in status_flags:
            status_map[node_id] = "down"
        elif "offline" in status_flags:
            status_map[node_id] = "offline"
        # else: online nodes will be handled later
    return status_map


def write_to_csv(all_nodes, status_map):
    timestamp = datetime.datetime.now().replace(microsecond=0)
    with open(CSV_FILE, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        for node in all_nodes:
            status = status_map.get(node, "online")
            writer.writerow([node, timestamp, status])


def main():
    all_nodes = get_all_nodes()
    output = run_remote_command()
    status_map = parse_output(output)
    write_to_csv(all_nodes, status_map)


if __name__ == "__main__":
    main()

