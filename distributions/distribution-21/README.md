# FLX Distribution #21

Distribution date: December 2022

- Start: November-15-2022 12:50pm UTC
- Cutoff-date: December-15-2022 12:50pm UTC

Results of the overall distribution are in `per_campaign.csv` for individual query results for each distribution and in `summed.csv` which is the final file used for generating the Merkle root for the distributor contract.

To combine the individual query results run `./run_combine.sh ./distributions/distribution-21/query-results`

### RAI/ETH UNI-V2 LP

30 FLX per day to RAI/ETH LPs on Uniswap.

- Period: From November-15-2022 12:50pm UTC Until December-15-2022 12:50pm UTC
- Query: Node script at https://github.com/reflexer-labs/lp-minter-reward-script

Total FLX distributed: 1200 FLX

### RAI/DAI UNI-V3 LP

60 FLX per day to RAI/DAI Uniswap v3 LPs at Redemption price that also minted RAI. See `https://docs.reflexer.finance/incentives/rai-uniswap-v3-mint-+-lp-incentives-program`

- Period: From November-15-2022 12:50pm UTC UTC Until December-15-2022 12:50pm UTC
- Query: Node script at https://github.com/reflexer-labs/uni-v3-incentive-reward-script

Total FLX distributed: 2400 FLX

### RAI Curve LP

30 FLX per day to RAI Curve LPs (RAI-3CRV LP token holders)

- Period: From November-15-2022 12:50pm UTC UTC Until December-15-2022 12:50pm UTC
- Query `curve.sql`
- Command `./run_incentives_query.sh ./distributions/distribution-21/queries/curve.sql distributions/distribution-21/query-results/curve.csv`

Total FLX distributed: 1200 FLX
