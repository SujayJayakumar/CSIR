# import_node_load.py
import pandas as pd
from psycopg2.extras import execute_batch
from db_config import get_connection

def standardize_node_id(node_id):
    """Convert r04gn01 → r04cn01 to match DB format"""
    return node_id.replace("gn", "cn")

# Load and preprocess
df = pd.read_csv("nodes_load_data.csv")
df = df.rename(columns=lambda x: x.strip())
df = df.rename(columns={'Timestamp': 'timestamp'})
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Standardize column names before melting
df.columns = [standardize_node_id(col) if col != 'timestamp' else col for col in df.columns]

# Melt to long format
melted = df.melt(id_vars=['timestamp'], var_name='node_id', value_name='load')

# DB insertion
conn = get_connection()
cur = conn.cursor()

execute_batch(
    cur,
    """
    INSERT INTO node_load (timestamp, node_id, load)
    VALUES (%s, %s, %s)
    ON CONFLICT (timestamp, node_id) DO NOTHING;
    """,
    melted.values.tolist()
)

conn.commit()
cur.close()
conn.close()
print("✅ Imported nodes_load_data.csv")
