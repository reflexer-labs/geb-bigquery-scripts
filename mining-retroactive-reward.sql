# SAFE Debt Modifications
# Config 
DECLARE LPTokenAddress DEFAULT "0x8ae720a71622e824f576b4a8c03031066548a3b1";      # UNI-V2-ETH/RAI address, lower case only
DECLARE SAFEManagerAddress DEFAULT "0xefe0b4ca532769a3ae758fd82e1426a03a94f185";  # GebSafeManager
DECLARE DeployDate DEFAULT TIMESTAMP("2021-02-13 00:00:00+00");                   # UTC date, Set it to just before the first ever LP token mint
DECLARE StartDate DEFAULT TIMESTAMP("2021-02-17 00:00:00+00");                    # UTC date, Set it to when to start to distribute rewards
DECLARE CutoffDate DEFAULT TIMESTAMP("2021-02-23 00:00:00+00");                   # UTC date, Set it to when to stop to distribute rewards
DECLARE TokenOffered DEFAULT 1000e18;                                             # Number of FLX to distribute in total
DECLARE ModifyCollTopic DEFAULT "0x4a1d86235388d42bee8b26817295ba354feb351780a0005e14a02303ac302df8"; # SAFE Manager ModifySAFECollateralization topic
# Constants
DECLARE NullAddress DEFAULT "0x0000000000000000000000000000000000000000";
DECLARE RewardRate DEFAULT TokenOffered / CAST(TIMESTAMP_DIFF(CutoffDate, StartDate, SECOND) AS NUMERIC);

CREATE TEMP FUNCTION
  PARSE_MODSAFE_LOG(data STRING, topics ARRAY<STRING>)
  RETURNS STRUCT<`sender` STRING, `deltaDebt` STRING>
  LANGUAGE js AS """
    var parsedEvent = {"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"safe","type":"uint256"},{"indexed":false,"internalType":"int256","name":"deltaCollateral","type":"int256"},{"indexed":false,"internalType":"int256","name":"deltaDebt","type":"int256"}],"name":"ModifySAFECollateralization","type":"event"}
    return abi.decodeEvent(parsedEvent, data, topics, false);
"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );
  
# Exclusion list of addresses that wont receive rewards, lower case only!
WITH excluded_list AS (
  SELECT * FROM UNNEST ([
    "0x9d5ab5758ac8b14bee81bbd4f019a1a048cf2246",
    "0x60efac991ae39fa6a594af58fd6fcb57940c3aa7"
    # TODO: Add addresses of exclusion list here
  ]) as address), 
  
# Get all ModifySAFECollateralization events from GebSafeManager
deltaDebts_raw AS (
  SELECT *, PARSE_MODSAFE_LOG(data, topics) as safeMod FROM `bigquery-public-data.crypto_ethereum.logs`
    WHERE block_timestamp >= DeployDate
      AND block_timestamp <= CutoffDate
      AND address = SAFEManagerAddress
      AND topics[offset(0)] = ModifyCollTopic
      
),
# Cast delta debt to BIGNUMERIC
deltaDebts as (
  SELECT block_timestamp, block_number, log_index, safeMod.sender as address, CAST(safeMod.deltaDebt as BIGNUMERIC) as deltaDebt from deltaDebts_raw
),

# Keep only records after the start date
deltaDebts_after AS (
  SELECT * FROM deltaDebts
  WHERE block_timestamp >= StartDate
),

# Process records before the start date like if everyone prior to strtDate had deposited on start date
deltaDebts_before AS (
  SELECT StartDate AS block_timestamp, MAX(block_number) AS block_number, 0 AS log_index, address, SUM(deltaDebt) AS deltaDebt FROM deltaDebts
  WHERE block_timestamp <= StartDate
  GROUP BY address
),

# Merge records from before and after
deltaDebts_on_start AS (
  SELECT block_timestamp, block_number, log_index, address, deltaDebt FROM deltaDebts_before
  UNION ALL 
  SELECT block_timestamp, block_number, log_index, address, deltaDebt FROM deltaDebts_after
),

# Exclude the addresses from the exclusion list
deltaDebts_with_exclusions AS (
SELECT * FROM deltaDebts_on_start
WHERE address NOT IN (SELECT address FROM excluded_list)
),

# Add total_debt and individual debt balances
total_debt_and_balances AS (
  SELECT * ,
    # Add total_supply of lp token by looking at the balance of 0x0
    SUM(deltaDebt) OVER(ORDER BY block_timestamp, log_index) AS total_debt,
    # Debt balance of each individual address
    SUM(deltaDebt) OVER(PARTITION BY address ORDER BY block_timestamp, log_index) AS balance
  FROM deltaDebts_with_exclusions
),

# Add the delta_reward_per_token (increase in reward_per_token)
deltaDebts_delta_reward_per_token AS (
  SELECT *, 
      COALESCE(CAST(TIMESTAMP_DIFF(block_timestamp, LAG(block_timestamp) OVER( ORDER BY block_timestamp, log_index), SECOND) AS NUMERIC), 0) AS delta_t,
      COALESCE(CAST(TIMESTAMP_DIFF(block_timestamp, LAG(block_timestamp) OVER( ORDER BY block_timestamp, log_index), SECOND) AS NUMERIC), 0) * RewardRate / (LAG(total_debt) OVER(ORDER BY block_timestamp, log_index)) AS delta_reward_per_token

  FROM total_debt_and_balances),
  
 deltaDebts_reward_per_token AS (
  SELECT *,
    SUM(delta_reward_per_token) OVER(ORDER BY block_timestamp, log_index) AS reward_per_token
  FROM deltaDebts_delta_reward_per_token
),

# Build a simple list of all paticipants
all_addresses AS (
  SELECT DISTINCT address FROM deltaDebts_reward_per_token
),

# Add cutoff events like if everybody had not debt on cutoff date. We need this to account for people that still have debt on cutoff date.
deltaDebts_with_cutoff_events AS (
  SELECT 
    block_timestamp, 
    log_index, 
    address, 
    balance,
    reward_per_token  
  FROM deltaDebts_reward_per_token
  
  UNION ALL  

  # Add the cutoff events
  SELECT
    CutoffDate AS block_timestamp,
    # Set it to the highest log index to be sure it comes last
    (SELECT MAX(log_index) FROM deltaDebts_reward_per_token) AS log_index,
    address AS address,
    # You unstaked so your balance is 0
    0 AS balance,
    # ⬇ reward_per_token on cutoff date                            ⬇ Time passed since the last update of reward_per_token                                                                              ⬇ latest total_supply
    (SELECT MAX(reward_per_token) FROM deltaDebts_reward_per_token) + COALESCE(CAST(TIMESTAMP_DIFF(CutoffDate, (SELECT MAX(block_timestamp) FROM deltaDebts_reward_per_token), SECOND) AS NUMERIC), 0) * RewardRate / (SELECT total_debt FROM deltaDebts_reward_per_token ORDER BY block_timestamp DESC LIMIT 1) 
    AS reward_per_token
  FROM all_addresses
),

# Credit rewards, basically the earned() function from a staking contract
deltaDebts_earned AS (
  SELECT *,
    #                       ⬇ userRewardPerTokenPaid                                                                             ⬇ balance just before 
    (reward_per_token - COALESCE(LAG(reward_per_token,1) OVER(PARTITION BY address ORDER BY block_timestamp, log_index), 0)) * COALESCE(LAG(balance) OVER(PARTITION BY address ORDER BY block_timestamp, log_index),0) AS earned,
  FROM deltaDebts_with_cutoff_events
),

# Sum up the earned event per address
final_reward_list AS (
  SELECT address, SUM(earned) AS reward
  FROM deltaDebts_earned
  GROUP BY address
)

# Output results
SELECT address, CAST(reward AS NUMERIC)/1e18 AS reward
FROM final_reward_list 
WHERE 
  address != NullAddress AND
  reward > 0
ORDER BY reward DESC
