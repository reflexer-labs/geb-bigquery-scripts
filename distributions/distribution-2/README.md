# FLX Distribution #2

Distribution date: TBD

- Start: April-13-2021 12:50pm UTC
- Cutoff-date: May-13-2021 12:50pm UTC

Results of the overall distribution are in `per_campaign.csv` for individual query results for each distribution and in `summed.csv` which is the final file used for generating the Merkle root for the distributor contract.

To combine the individual query results run `./run_combine.sh ./distributions/distribution-2/query-results`

## Individual distributions

### RAI RAI/ETH UNI-V2 LP

233.8 FLX per day to RAI/ETH LPs on Uniswap that also minted RAI. See `https://docs.reflexer.finance/incentives/rai-mint-+-lp-incentives-program`

- Period: From April-13-2021 12:50pm UTC Until May-13-2021 12:50pm UTC
- Query: Node script at https://github.com/reflexer-labs/lp-minter-reward-script

Total FLX distributed: 7014 FLX

### RAI RAI/DAI UNI-V2 LP

100.2 FLX per day to RAI/ETH LPs on Uniswap that also minted RAI. See `https://docs.reflexer.finance/incentives/rai-mint-+-lp-incentives-program`

- Period: From April-13-2021 12:50pm UTC Until May-13-2021 12:50pm UTC
- Query: Node script at https://github.com/reflexer-labs/lp-minter-reward-script

Total FLX distributed: 3066 FLX

### Cream borrower

15 FLX per day to RAI borrowers pro rata to the borrow amount.

- Period: From April-13-2021 12:50pm UTC Until May-13-2021 12:50pm UTC
- Query `cream-borrower.sql`
- Command `./run_incentives_query.sh ./distributions/distribution-2/queries/cream-borrower.sql exclusions.csv distributions/distribution-2/query-results/cream-borrower.csv`

Total FLX distributed: 450 FLX

### Fuse borrower

15 FLX per day to RAI borrowers pro rata to the borrow amount.

- Period: From April-14-2021 12:50pm UTC Until May-13-2021 12:50pm UTC
- Query `fuse-borrower.sql`
- Command `./run_incentives_query.sh ./distributions/distribution-2/queries/fuse-borrower.sql exclusions.csv distributions/distribution-2/query-results/fuse-borrower.csv`

Total FLX distributed: 435 FLX

### Loopring LP

TODO