# Config 
DECLARE LPTokenAddress DEFAULT "0xebde9f61e34b7ac5aae5a4170e964ea85988008c";   # RAI address, lower case only
DECLARE StartDate DEFAULT TIMESTAMP("2020-10-26 18:11:20+00");                 # UTC date, Set it to the first ever LP token mint
DECLARE CutoffDate DEFAULT TIMESTAMP("2021-01-25 12:43:21+00");                # UTC date, after this date we won't give anymore rewards
DECLARE TokenOffered DEFAULT 1000e18;                                          # Number of FLX to distribute in total

# Constants
DECLARE NullAddress DEFAULT "0x0000000000000000000000000000000000000000";
DECLARE RewardRate DEFAULT TokenOffered / CAST(TIMESTAMP_DIFF(CutoffDate, StartDate, SECOND) AS NUMERIC);

# Fetch the list of all RAI LP token transfers (includes mint & burns)
WITH raw_lp_transfers as (
  SELECT * FROM `bigquery-public-data.crypto_ethereum.token_transfers`
  WHERE block_timestamp >= StartDate 
    AND block_timestamp <= CutoffDate
    AND token_address = LPTokenAddress
    # Parasite transaction
    AND (from_address != NullAddress OR to_address != NullAddress)
), 

# Calculate the realtive LP token balance delta, outgoing transfer is negative delta
lp_transfers_deltas as (
  SELECT * FROM (
    SELECT block_timestamp, block_number, log_index, from_address as address, -1 * CAST(value AS NUMERIC) AS delta_lp FROM raw_lp_transfers
    UNION ALL 
    SELECT block_timestamp, block_number, log_index, to_address as address, CAST(value AS NUMERIC) AS delta_lp FROM raw_lp_transfers
  )
),

# Add lp token total_supply and individual balances
lp_total_supply_and_balances as (
  SELECT * ,
    # Add total_supply of lp token by looking at the balance of 0x0
    SUM(CASE WHEN address = NullAddress THEN -1 * delta_lp ELSE 0 END) OVER(ORDER BY block_timestamp, log_index) as total_supply,
    # LP balance of each individual address
    SUM(delta_lp) OVER(PARTITION BY address ORDER BY block_timestamp, log_index) as balance
  FROM lp_transfers_deltas
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
SELECT address, CAST(reward AS NUMERIC) AS reward
FROM final_reward_list 
WHERE 
  address != NullAddress AND
  reward > 0
ORDER BY reward DESC