import json
import requests
import pandas as pd
from google.cloud import bigquery

def get_safe_owners(graph_url, block_number):
    query = '''
        query {{
        safeHandlerOwners(first: 1000, skip: {}, block: {{number:{}}}) {{
          id  
          owner {{
            address
          }}  
        }}  
        }}  
        ''' 

    n = 0
    results = []
    while True:
        r = requests.post(graph_url, json = {'query':query.format(n*1000, block_number)})
        try:
            s = json.loads(r.content)['data']['safeHandlerOwners']
        except:
            print(json.loads(r.content))
            break
        results.extend([(block_number, x['id'], x['owner']['address']) for x in s])
        n += 1
        if len(s) < 1000:
            break
    return pd.DataFrame(results, columns=['block', 'safe', 'owner'])

def write_csv_to_bq(client, table_id, schema, csv_file):
    client.delete_table(table_id, not_found_ok=True)

    job_config = bigquery.LoadJobConfig(
            schema=schema,
            #skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )

    with open(csv_file, 'rb') as f:
        load_job = client.load_table_from_file(
            f, table_id, job_config=job_config
        )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows to {}".format(destination_table.num_rows, table_id))
