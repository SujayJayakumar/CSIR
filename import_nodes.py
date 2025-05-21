import psycopg2
from psycopg2.extras import execute_batch

def racknode_to_rXcnYY(rack, node):
    return f"r{rack:02d}cn{node:02d}"

conn = psycopg2.connect(
    dbname="csir",
    user="csiruser",
    password="password",
    host="localhost"
)
cur = conn.cursor()

data = []
for rack in range(1, 14):  # racks 1 to 13
    for node in range(1, 39):  # nodes 1 to 38
        if((rack==4 and node==7) or (rack==5 and node==7)):
            break
        node_id = racknode_to_rXcnYY(rack, node)
        is_gpu = rack in [4, 5]
        core_count = 4 if is_gpu else 2
        data.append((node_id, core_count, is_gpu))

execute_batch(
    cur,
    "INSERT INTO nodes (node_id, core_count, is_gpu) VALUES (%s, %s, %s) ON CONFLICT (node_id) DO NOTHING",
    data
)

conn.commit()
cur.close()
conn.close()

print("✅ Nodes table populated using rXcnYY format.")
