import sys
import os
from google.cloud import bigquery
from util import write_csv_to_bq

BQ_EXCLUDED_OWNERS = 'exclusions.excluded_owners' # Must match in prai .sql file
GOOGLE_AUTH = os.environ['GOOGLE_AUTH']

query_file = sys.argv[1]
exclusion_file = sys.argv[2]
output_file = sys.argv[3]

if len(sys.argv) != 4:
    print("Usage: python incentives_query.py <query_file> <exclusions_file> <output_file>")
    sys.exit()

with open(query_file, 'r') as file:
    query = file.read()

# setup bq client
client = bigquery.Client.from_service_account_json(GOOGLE_AUTH)

# Load and write exclusions to BQ
exclusion_schema=[bigquery.SchemaField("address", "STRING", "REQUIRED")]
write_csv_to_bq(client, BQ_EXCLUDED_OWNERS, exclusion_schema, exclusion_file)

# Run BQ Query
parent_job = client.query(query)
rows_iterable = parent_job.result()

# Fetch BQ results.
rows = list(rows_iterable)

print(f"{len(rows)} final addresses")

# Write results to file
with open(output_file, 'w') as f:
    for row in rows:
        f.write(f"{'='.join(map(str, row))}\n")
