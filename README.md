# Incentives queries

 Get google auth key and set environment variable.
```
export GOOGLE_AUTH=<key file>
```

Update `exclusions.csv` with excluded EOAs, if needed.
Double-check cutoff dates in query file.


## Running
This is the general format of the command

```
./run_incentives_query.sh <query_file> <exclusions_file> <output_file>
./run_incentives_query_with_owner_mapping.sh  <query_file> <exclusions_file> <output_file>
```

Commands used to generate the distributions:
```
./run_incentives_query.sh queries/prai.sql exclusions.csv final_output/individual_query_results/prai.csv
./run_incentives_query.sh queries/lp-reward-1.sql exclusions.csv final_output/individual_query_results/lp-reward-1.csv
./run_incentives_query.sh queries/lp-reward-2.sql exclusions.csv final_output/individual_query_results/lp-reward-2.csv
./run_incentives_query.sh queries/lp-reward-3.sql exclusions.csv final_output/individual_query_results/lp-reward-3.csv
./run_incentives_query_with_owner_mapping.sh queries/minting-reward-1.sql exclusions.csv final_output/individual_query_results/minting-reward-1.csv
./run_incentives_query_with_owner_mapping.sh queries/flat-reward.sql exclusions.csv final_output/individual_query_results/flat-reward-1.csv

```
The `mint-lp-rewards-1.csv` was generated from another repo.