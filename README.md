# Incentives queries


To run a query, get a GCP service account key file and set its path in as environment:
```
export GOOGLE_AUTH=<key file>
```
Alternatively, copy the query and paste it in the Google BigQuery online editor: https://console.cloud.google.com/bigquery

Update `exclusions.csv` with excluded EOAs, if needed. Note that not all scripts supports address exclusion. 


## Running
This is the general format of the command

```
./run_incentives_query.sh <query_file> <exclusions_file> <output_file>
./run_incentives_query_with_owner_mapping.sh  <query_file> <exclusions_file> <output_file>
```