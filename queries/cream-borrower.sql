# Config 
DECLARE DeployDate DEFAULT TIMESTAMP("2021-03-24 17:48:00+00"); # UTC date, Set it to just before the first ever LP token mint
DECLARE StartDate DEFAULT TIMESTAMP("2021-03-31 12:50:00+00"); # UTC date, Set it to when to start to distribute rewards
DECLARE CutoffDate DEFAULT TIMESTAMP("2021-04-02 12:50:00+00"); # UTC date, Set it to when to stop to distribute rewards
DECLARE CrRaiAddress DEFAULT "0xf8445c529d363ce114148662387eba5e62016e20"; # Cream RAI CToken contract
DECLARE BorrowTopic DEFAULT "0x13ed6866d4e1ee6da46f845c46d7e54120883d75c5ea9a2dacc1c4ca8984ab80"; # Borrow event topic0
DECLARE RepayBorrowTopic DEFAULT "0x1a2a22cb034d26d1854bdc6666a5b91fe25efbbb5dcad3b0355478d6f5c362a1"; # Repay event topic0
DECLARE TokenOffered DEFAULT 1000e18; # Number of FLX to distribute in total

# Constants
DECLARE RewardRate DEFAULT TokenOffered / CAST(TIMESTAMP_DIFF(CutoffDate, StartDate, SECOND) AS NUMERIC); # FLX dsitributed per second

# Borrow event parse function
CREATE TEMP FUNCTION
  PARSE_BORROW_EVENT(data STRING, topics ARRAY<STRING>)
  RETURNS STRUCT<`borrower` STRING, `accountBorrows` STRING, `totalBorrows` STRING, `borrowAmount` STRING>
  LANGUAGE js AS """
    var parsedEvent = {"anonymous": false,"inputs": [{"indexed": false,"internalType": "address","name": "borrower","type": "address"},{"indexed": false,"internalType": "uint256","name": "borrowAmount","type": "uint256"},{"indexed": false,"internalType": "uint256","name": "accountBorrows","type": "uint256"},{"indexed": false,"internalType": "uint256","name": "totalBorrows","type": "uint256"}],"name": "Borrow","type": "event"};
    return abi.decodeEvent(parsedEvent, data, topics, false);
"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

# Repay event parse function
CREATE TEMP FUNCTION
  PARSE_REPAY_BORROW_EVENT(data STRING, topics ARRAY<STRING>)
  RETURNS STRUCT<`borrower` STRING, `accountBorrows` STRING, `totalBorrows` STRING, `repayAmount` STRING>
  LANGUAGE js AS """
    var parsedEvent = {"anonymous": false,"inputs": [{"indexed": false,"internalType": "address","name": "payer","type": "address"},{"indexed": false,"internalType": "address","name": "borrower","type": "address"},{"indexed": false,"internalType": "uint256","name": "repayAmount","type": "uint256"},{"indexed": false,"internalType": "uint256","name": "accountBorrows","type": "uint256"},{"indexed": false,"internalType": "uint256","name": "totalBorrows","type": "uint256"}],"name": "RepayBorrow","type": "event"};
    return abi.decodeEvent(parsedEvent, data, topics, false);
"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );


# Get all borrow and repay borrow events from RAI Cream CToken contract
WITH cream_raw_events AS (
  SELECT data, topics, block_timestamp, log_index FROM `bigquery-public-data.crypto_ethereum.logs`
    WHERE block_timestamp >= DeployDate
      AND block_timestamp <= CutoffDate
      AND address = CrRaiAddress
      AND (topics[offset(0)] = BorrowTopic OR topics[offset(0)] = RepayBorrowTopic)
),

# Parse the borrows
borrow_event AS (
  SELECT *, PARSE_BORROW_EVENT(data, topics) as params 
  FROM cream_raw_events 
  WHERE topics[offset(0)] = BorrowTopic
),

# Parse the repays 
repay_borrow_event AS (
  SELECT *, PARSE_REPAY_BORROW_EVENT(data, topics) as params 
  FROM cream_raw_events 
  WHERE topics[offset(0)] = RepayBorrowTopic
),

# Union borrows and repays
cream_parsed_events AS (
  SELECT 
    block_timestamp, 
    log_index, params.borrower as address, 
    CAST(params.accountBorrows as BIGNUMERIC) as balance, 
    CAST(params.totalBorrows as BIGNUMERIC) as total_supply,
    CAST(params.borrowAmount as BIGNUMERIC) as delta_balance,
  FROM borrow_event
  
  UNION ALL
  
  SELECT 
    block_timestamp, 
    log_index, 
    params.borrower as address, 
    CAST(params.accountBorrows as BIGNUMERIC) as balance, 
    CAST(params.totalBorrows as BIGNUMERIC) as total_supply,
    -1 * CAST(params.repayAmount as BIGNUMERIC) as delta_balance,
  FROM repay_borrow_event
),

# Keep only records after the start date
cream_parsed_events_before_start AS (
  SELECT * FROM cream_parsed_events
  WHERE block_timestamp < StartDate
),

# Total supply (= total debt) at the start of the distribution
# !! This ignore the accrued intrest since the last borrow/repay event until the start of the distribution
initial_total_supply as (
  SELECT total_supply 
  FROM  (
    SELECT 
      total_supply, 
      RANK() OVER (ORDER BY block_timestamp DESC, log_index DESC) as rank 
    FROM cream_parsed_events_before_start) 
  WHERE rank=1
),

# Get the initial balances (=debt) for each address
# !! This ignore the accrued intrest since the last borrow/repay event of the address until the start of the distribution
cream_balance_before_start AS (
  SELECT 
    StartDate AS block_timestamp,
    # Set it to -1 to be sure it comes before everything else in the start block
    -1 AS log_index,
    address,
    balance AS balance,
    (SELECT total_supply FROM initial_total_supply) as total_supply,
    # Delta_balance is the full balance at start
    balance AS delta_balance,
  FROM (
    SELECT 
      balance, 
      address,
      RANK() OVER (PARTITION BY address ORDER BY block_timestamp DESC, log_index DESC) as rank
    FROM cream_parsed_events_before_start
  ) WHERE rank=1
),



# Add initial balance events for all account
with_start_events AS (
  SELECT block_timestamp, log_index, address, balance, total_supply, delta_balance
  FROM cream_parsed_events
  WHERE block_timestamp >= StartDate
  
  UNION ALL
  SELECT * FROM cream_balance_before_start

),

# Add the delta_reward_per_token (increase in reward_per_token)
cream_deltas AS (
  SELECT *, 
      COALESCE(CAST(TIMESTAMP_DIFF(block_timestamp, LAG(block_timestamp) OVER( ORDER BY block_timestamp, log_index), SECOND) AS NUMERIC), 0) AS delta_t,
      COALESCE(CAST(TIMESTAMP_DIFF(block_timestamp, LAG(block_timestamp) OVER( ORDER BY block_timestamp, log_index), SECOND) AS NUMERIC), 0) * RewardRate / (total_supply - delta_balance) AS delta_reward_per_token
  FROM with_start_events),

# Calculated the actual reward_per_token from the culmulative delta
cream_reward_per_token AS (
  SELECT *,
    SUM(delta_reward_per_token) OVER(ORDER BY block_timestamp, log_index) AS reward_per_token
  FROM cream_deltas
),

# Build a simple list of all paticipants
-- all_addresses AS (
--   SELECT DISTINCT address FROM cream_reward_per_token
-- ),

final_balance as (
  SELECT 
    address,
    balance,
  FROM  (
    SELECT
      address,
      balance,
      RANK() OVER (PARTITION BY address ORDER BY block_timestamp DESC, log_index DESC) as rank 
    FROM cream_reward_per_token
   ) 
  WHERE rank=1
),

# Add cutoff events like if everybody had repay everything on cutoff date. We need this to account for people that are still have balance on cutoff date.
with_cutoff_events AS (
  SELECT 
    block_timestamp, 
    log_index, 
    address, 
    balance,
    reward_per_token,
    delta_balance,
  FROM cream_reward_per_token
  
  UNION ALL  

  # Add the cutoff events
  SELECT
    CutoffDate AS block_timestamp,
    # Set it to the highest log index to be sure it comes last
    (SELECT MAX(log_index) FROM cream_reward_per_token) AS log_index,
    address AS address,
    # You repay so your balance is 0
    0 AS balance,
    # ⬇ reward_per_token on cutoff date                            ⬇ Time passed since the last update of reward_per_token                                                                                   ⬇ latest total_supply !! This ignore accrued intrest between the last change and Cutoff date
    (SELECT MAX(reward_per_token) FROM cream_reward_per_token) + COALESCE(CAST(TIMESTAMP_DIFF(CutoffDate, (SELECT MAX(block_timestamp) FROM cream_reward_per_token), SECOND) AS NUMERIC), 0) * RewardRate / (SELECT total_supply FROM cream_reward_per_token ORDER BY block_timestamp DESC LIMIT 1) AS reward_per_token,
    # You repay everything so your delta is equal -1 * currentBalance
    -1 * balance as delta_balance
  FROM final_balance
),

# Credit rewards, basically the earned() function from a staking contract
cream_earned AS (
  SELECT *,
    #                       ⬇ userRewardPerTokenPaid                                                                             ⬇ balance just before 
    (reward_per_token - COALESCE(LAG(reward_per_token,1) OVER(PARTITION BY address ORDER BY block_timestamp, log_index), 0)) * COALESCE(balance - delta_balance, 0) AS earned,
  FROM with_cutoff_events
),

# Sum up the earned event per address
final_reward_list AS (
  SELECT address, SUM(earned) AS reward
  FROM cream_earned
  GROUP BY address
)

# Output results
SELECT address, CAST(reward AS NUMERIC)/1e18 AS reward
FROM final_reward_list 
ORDER BY reward DESC

# SELECT * FROM final_balance # ORDER BY block_timestamp
