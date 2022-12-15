import sys
import os
from google.cloud import bigquery
from util import write_csv_to_bq

GOOGLE_AUTH = os.environ['GOOGLE_AUTH']

query_file = sys.argv[1]
output_file = sys.argv[2]

if len(sys.argv) != 3:
    print("Usage: python incentives_query.py <query_file> <output_file>")
    sys.exit()

with open(query_file, 'r') as file:
    query = file.read()

# setup bq client
client = bigquery.Client.from_service_account_json(GOOGLE_AUTH)

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
