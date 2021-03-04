# Incentives queries

 Get google auth key and set environment variable.
```
export GOOGLE_AUTH=<key file>
```

Update `exclusions.csv` if needed.
Double-check cutoff dates in query file.


## Running
This is the general command, except for minting incentives(see below)

`./run_incentives_query.sh <query_file> <exclusions_file> <output_file>`

### Run PRAI query
```
./run_incentives_query.sh queries/prai_airdrop_query.sql exclusions.csv prai.out
```

### Run LP Staking for Period 1
```
./run_incentives_query.sh staking-retroactive-reward-period1.sql exclusions.csv staking1.out

```

### Run LP Staking for Period 2

Set CutoffDate in `staking-retroactive-reward-period1.sql` when period ends
```
./run_incentives_query.sh staking-retroactive-reward-period1.sql exclusions.csv staking2.out

```

### Run Minting only incentives

*Minting only queries needs to map owners to SAFEs first, so it uses a separate script.*


```
./run_incentives_query.sh minting-retroactive-reward.sql exclusions.csv minting.out

```
