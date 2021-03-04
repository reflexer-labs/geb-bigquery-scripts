# Config 
DECLARE LPTokenAddress DEFAULT "0x8ae720a71622e824f576b4a8c03031066548a3b1";   # UNI-V2-ETH/RAI address, lower case only
DECLARE DeployDate DEFAULT TIMESTAMP("2021-02-13 00:00:00+00");                # UTC date, Set it to just before the first ever LP token mint
DECLARE StartDate DEFAULT TIMESTAMP("2021-02-17 00:00:00+00");                 # UTC date, Set it to when to start to distribute rewards
DECLARE CutoffDate DEFAULT TIMESTAMP("2021-02-20 00:00:00+00");                # UTC date, Set it to when to stop to distribute rewards
DECLARE TokenOffered DEFAULT 1000e18;                                          # Number of FLX to distribute in total

# Constants
DECLARE NullAddress DEFAULT "0x0000000000000000000000000000000000000000";
DECLARE RewardRate DEFAULT TokenOffered / CAST(TIMESTAMP_DIFF(CutoffDate, StartDate, SECOND) AS NUMERIC);

# Exclusion list of addresses that wont receive rewards, lower case only!
WITH excluded_list AS (
  SELECT * FROM UNNEST ([
    "0x0ce1ff652be78322e312e5073cd96b5e1cf5306e",
    "0x3e0139ce3533a42a7d342841aee69ab2bfee1d51",
    "0x45c9a201e2937608905fef17de9a67f25f9f98e0",
    "0xbd3f90047b14e4f392d6877276d52d0ac59f4cf8",
    "0x935a301ba674816524ceb4b1eabddb96c57ab805",
    "0x6779122d59efdd6ec048fd5de02c2904ccffa259",
    "0xa5ccb4286355b3412f1487aa52f5db93307aeaf7",
    "0xdf8f5cf7a2959f62009c655c896d3c0c6364d7d6",
    "0x99fb4386310756522e727388bf5b68ccfaa22247",
    "0x6048cd849a6a1364a54a09f7cf430724695bbd0c",
    "0xa7691fc42dcba2efecd73675f90f119fcf1b6373",
    "0xe8d944108afce391cdb7a0d90257e854c07fd918",
    "0x2b6216d0b1734cb73fdfd4bde616b761a3bddccf",
    "0x4d1fb7a1aa8df65c169e76788baf4b68a72fca96",
    "0xdd1693bd8e307ecfdbe51d246562fc4109f871f8",
    "0xa346a2ed29750e8399a787946fabe06e81a39f3b",
    "0x60efac991ae39fa6a594af58fd6fcb57940c3aa7",
    "0x02b70c78b400ff8fe89af7d84d443f875d047a8f",
    "0x871e1e0b7cdbc56ed8b682641158238562ca9ee4",
    "0x953d1613063e9f3a5fb5cba849166d4d12992ccd",
    "0xb9f4879d53259bde15a92b78d0da1c9f29767332",
    "0x25f952c6b87d3a9c48ac86b61f27b81a6f2ed332",
    "0x08717dc665247452454b6976a0fc6aab3a97d31f",
    "0xb193044b986956791cab713ff3cf9c1c474f2247",
    "0xf685e3819ad71772b4715425ba40e477b1d5d6bd",
    "0x4bea44985095bb98deef727ecc3509c9edfb1b19",
    "0x1d28a17529216cf013f62ecb889b874d65c9a55c",
    "0x9a52b4b63b1a4c9b25268254fac82001d2e687e1",
    "0x297bf847dcb01f3e870515628b36eabad491e5e8",
    "0x0cb27e883e207905ad2a94f9b6ef0c7a99223c37",
    "0xfa5e4955a11902f849ecaddef355db69c2036de6",
    "0xe189bbb764ff0614cc608e09a49cc7569ff94521",
    "0x94eed5e9ec2b85dbfa018d6978ee7e4126ad5134"
  ]) as address),

# Fetch the list of all RAI LP token transfers (includes mint & burns)
raw_lp_transfers AS (
  SELECT * FROM `bigquery-public-data.crypto_ethereum.token_transfers`
  WHERE block_timestamp >= DeployDate 
    AND block_timestamp <= CutoffDate
    AND token_address = LPTokenAddress
    # Parasite transaction
    AND (from_address != NullAddress OR to_address != NullAddress)
), 

# Calculate the realtive LP token balance delta, outgoing transfer is negative delta
lp_transfers_deltas AS (
  SELECT * FROM (
    SELECT block_timestamp, block_number, log_index, from_address AS address, -1 * CAST(value AS NUMERIC) AS delta_lp FROM raw_lp_transfers
    UNION ALL 
    SELECT block_timestamp, block_number, log_index, to_address AS address, CAST(value AS NUMERIC) AS delta_lp FROM raw_lp_transfers
  )
),

# Keep only records after the start date
lp_transfers_deltas_after AS (
  SELECT * FROM lp_transfers_deltas
  WHERE block_timestamp >= StartDate
),

# Process records before the start date like if everyone prior to strtDate had deposited on start date
lp_transfers_deltas_before AS (
  SELECT  StartDate AS block_timestamp, MAX(block_number) AS block_number, 0 AS log_index, address, SUM(delta_lp) AS delta_lp FROM lp_transfers_deltas
  WHERE block_timestamp <= StartDate
  GROUP BY address
),

# Merge records from before and after
lp_transfers_deltas_on_start AS (
  SELECT block_timestamp, block_number, log_index, address, delta_lp FROM lp_transfers_deltas_before
  UNION ALL 
  SELECT block_timestamp, block_number, log_index, address, delta_lp FROM lp_transfers_deltas_after
),

# Exclude the addresses from the exclusion list
lp_with_exclusions AS (
SELECT * FROM lp_transfers_deltas_on_start
WHERE address NOT IN (SELECT address FROM excluded_list)
),

# Add lp token total_supply and individual balances
lp_total_supply_and_balances AS (
  SELECT * ,
    # Add total_supply of lp token by looking at the balance of 0x0
    SUM(CASE WHEN address = NullAddress THEN -1 * delta_lp ELSE 0 END) OVER(ORDER BY block_timestamp, log_index) AS total_supply,
    # LP balance of each individual address
    SUM(delta_lp) OVER(PARTITION BY address ORDER BY block_timestamp, log_index) AS balance
  FROM lp_with_exclusions
),

# Add the delta_reward_per_token (increase in reward_per_token)
lp_delta_reward_per_token AS (
  SELECT *, 
      COALESCE(CAST(TIMESTAMP_DIFF(block_timestamp, LAG(block_timestamp) OVER( ORDER BY block_timestamp, log_index), SECOND) AS NUMERIC), 0) AS delta_t,
      COALESCE(CAST(TIMESTAMP_DIFF(block_timestamp, LAG(block_timestamp) OVER( ORDER BY block_timestamp, log_index), SECOND) AS NUMERIC), 0) * RewardRate / (LAG(total_supply) OVER(ORDER BY block_timestamp, log_index)) AS delta_reward_per_token

  FROM lp_total_supply_and_balances),

# Calculated the actual reward_per_token from the culmulative delta
lp_reward_per_token AS (
  SELECT *,
    SUM(delta_reward_per_token) OVER(ORDER BY block_timestamp, log_index) AS reward_per_token
  FROM lp_delta_reward_per_token
),

# Build a simple list of all paticipants
all_addresses AS (
  SELECT DISTINCT address FROM lp_reward_per_token
),

# Add cutoff events like if everybody had unstaked on cutoff date. We need this to account for people that are still staking on cutoff date.
lp_with_cutoff_events AS (
  SELECT 
    block_timestamp, 
    log_index, 
    address, 
    balance,
    reward_per_token  
  FROM lp_reward_per_token
  
  UNION ALL  

  # Add the cutoff events
  SELECT
    CutoffDate AS block_timestamp,
    # Set it to the highest log index to be sure it comes last
    (SELECT MAX(log_index) FROM lp_reward_per_token) AS log_index,
    address AS address,
    # You unstaked so your balance is 0
    0 AS balance,
    # ⬇ reward_per_token on cutoff date                            ⬇ Time passed since the last update of reward_per_token                                                                              ⬇ latest total_supply
    (SELECT MAX(reward_per_token) FROM lp_reward_per_token) + COALESCE(CAST(TIMESTAMP_DIFF(CutoffDate, (SELECT MAX(block_timestamp) FROM lp_reward_per_token), SECOND) AS NUMERIC), 0) * RewardRate / (SELECT total_supply FROM lp_reward_per_token ORDER BY block_timestamp DESC LIMIT 1) 
    AS reward_per_token
  FROM all_addresses
),

# Credit rewards, basically the earned() function from a staking contract
lp_earned AS (
  SELECT *,
    #                       ⬇ userRewardPerTokenPaid                                                                             ⬇ balance just before 
    (reward_per_token - COALESCE(LAG(reward_per_token,1) OVER(PARTITION BY address ORDER BY block_timestamp, log_index), 0)) * COALESCE(LAG(balance) OVER(PARTITION BY address ORDER BY block_timestamp, log_index),0) AS earned,
  FROM lp_with_cutoff_events
),

# Sum up the earned event per address
final_reward_list AS (
  SELECT address, SUM(earned) AS reward
  FROM lp_earned
  GROUP BY address
)

# Output results
SELECT address, CAST(reward AS NUMERIC)/1e18 AS reward
FROM final_reward_list 
WHERE 
  address != NullAddress AND
  reward > 0
ORDER BY reward DESC
