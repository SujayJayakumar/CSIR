# import_node_summary.py
import pandas as pd
from psycopg2.extras import execute_batch
from db_config import get_connection

df = pd.read_csv("PBS_node_status_random.csv")
df['timestamp'] = pd.to_datetime(df['timestamp'])

conn = get_connection()
cur = conn.cursor()

execute_batch(
    cur,
    """
    INSERT INTO node_status (timestamp, total_nodes, online, offline, down)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (timestamp) DO NOTHING;
    """,
    df.values.tolist()
)

conn.commit()
cur.close()
conn.close()
print("✅ Imported PBS_node_status_random.csv")
