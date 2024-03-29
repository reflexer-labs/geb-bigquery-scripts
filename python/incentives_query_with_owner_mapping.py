import sys
import os
import re
import pandas as pd
from google.cloud import bigquery
from util import get_safe_owners, write_csv_to_bq

GRAPH_URL = 'https://api.thegraph.com/subgraphs/name/reflexer-labs/rai-mainnet'
BQ_SAFEOWNERS = 'safe_owners.safe_owners'
GOOGLE_AUTH = os.environ['GOOGLE_AUTH']

query_file = sys.argv[1]
output_file = sys.argv[2]

if len(sys.argv) != 3:
    print("Usage: python test.py <query_file> <output_file>")
    sys.exit()

with open(query_file, 'r') as file:
    query = file.read()

# Find the CutoffBlock in the query to populate the owner <> safe mapping at that block
match = re.search('DECLARE CutoffBlock DEFAULT (\d+)', query)
if match:
    CUTOFF_BLOCK = int(match.group(1))

# setup bq client
client = bigquery.Client.from_service_account_json(GOOGLE_AUTH)

# Get owners from graph and write to BQ. The query uses these to map safe->owner
safe_owners = get_safe_owners(GRAPH_URL, CUTOFF_BLOCK)
safe_owners.to_csv("safe_owners.csv", header=False , index=False)

owner_schema=[bigquery.SchemaField("block", "INT64", "REQUIRED"),
              bigquery.SchemaField("safe", "STRING", "REQUIRED"),
              bigquery.SchemaField("owner", "STRING", "REQUIRED")]
write_csv_to_bq(client, BQ_SAFEOWNERS, owner_schema, "safe_owners.csv")

# Run BQ Query
parent_job = client.query(query)
rows_iterable = parent_job.result()

# Fetch BQ results.
rows = list(rows_iterable)

print(f"{len(rows)} final addresses")

# Write results to file
with open(output_file, 'w') as f:
    for row in rows:
        f.write(f"{','.join(map(str, row))}\n")
