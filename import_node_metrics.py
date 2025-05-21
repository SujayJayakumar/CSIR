import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from db_config import get_connection

# Helper: Convert 1_1 → r01cn01
def format_node_id(node):
    rack, node_num = map(int, node.split('_'))
    return f"r{rack:02d}cn{node_num:02d}"

# Config
CSV_FILE = "hardware_temp_pow_data_compressed.csv"

# Load and preprocess
df = pd.read_csv(CSV_FILE)
df = df.rename(columns=lambda x: x.strip())
df['Timestamp'] = pd.to_datetime(df['Timestamp'])

# Extract node ID roots from column names like "1_1-CPU1"
metric_cols = df.columns[1:]  # Skip 'Timestamp'
raw_node_ids = sorted(set(col.rsplit('-', 1)[0] for col in metric_cols))

# Prepare records for insertion
rows = []
for idx, row in df.iterrows():
    ts = row["Timestamp"]
    for raw_id in raw_node_ids:
        cpu1_col = f"{raw_id}-CPU1"
        cpu2_col = f"{raw_id}-CPU2"
        power_col = f"{raw_id}-Pow-consumption"
        if all(col in df.columns for col in [cpu1_col, cpu2_col, power_col]):
            formatted_id = format_node_id(raw_id)
            rows.append((
                ts,
                formatted_id,
                row[cpu1_col],
                row[cpu2_col],
                row[power_col]
            ))

# Insert into PostgreSQL
insert_query = """
INSERT INTO node_metrics (timestamp, node_id, cpu1_temp, cpu2_temp, power_consumption)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (timestamp, node_id) DO NOTHING;
"""

try:
    conn = get_connection()
    cur = conn.cursor()
    execute_batch(cur, insert_query, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Inserted {len(rows)} records into node_metrics.")
except Exception as e:
    print("❌ Error during DB operation:", e)
