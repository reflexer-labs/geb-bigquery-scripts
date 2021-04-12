# Incentives queries

 Get google auth key and set environment variable.
```
export GOOGLE_AUTH=<key file>
```

Update `exclusions.csv` with excluded EOAs, if needed.


## Running
This is the general format of the command

```
./run_incentives_query.sh <query_file> <exclusions_file> <output_file>
./run_incentives_query_with_owner_mapping.sh  <query_file> <exclusions_file> <output_file>
```