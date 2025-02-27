{{
    config(
        materialized="table",
        snowflake_warehouse="EIGENLAYER",
        database="EIGENLAYER",
        schema="RAW",
        alias="fact_restaked_native_eth",
    )
}}


--current validators with their withdrawal address, at the most recent snapshot of the validator table (highest slot number)
WITH ValidatorsCurrent AS ( 
  SELECT
      index AS validator_index,
      '0x' || RIGHT(withdrawal_credentials, 40) AS withdrawal_address,
      slot_number,
      validator_status,
      effective_balance,
      modified_timestamp,
      DATE_TRUNC('day', inserted_timestamp) AS day_      
  FROM {{ source("ETHEREUM_FLIPSIDE_BEACON", "fact_validators")}} v -- flipside table 'ethereum.beacon_chain.fact_validators
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY index, withdrawal_credentials, day_
      ORDER BY slot_number DESC
  ) = 1
), 

--all eigenlayer pod addresses compiled from eigenlayer pod deployed events
EigenPods AS ( 
    SELECT 
        decoded_log:eigenPod::STRING AS eigenpod_address,
        DATE_TRUNC('day', block_timestamp) AS day_of_pod_deployed_event
    FROM 
        {{ source("ETHEREUM_FLIPSIDE", "fact_decoded_event_logs")}}-- flipside table 'ethereum.core.fact_decoded_event_logs'
    WHERE 
        contract_address = lower('0x91E677b07F7AF907ec9a428aafA9fc14a0d3A338')
        AND event_name = 'PodDeployed'
), 

--sum of all effective balance of validators that are restaked on eigenlayer pods
DailyRestakedNativeETH AS (
    SELECT 
        v.day_ AS date,
        v.validator_index,
        SUM(v.effective_balance) AS restaked_native_eth,
        'ethereum' AS chain,
        'eigenlayer' AS protocol
    FROM ValidatorsCurrent v
    INNER JOIN EigenPods e 
        ON e.eigenpod_address = v.withdrawal_address
        AND e.day_of_pod_deployed_event <= v.day_  -- ensures we only count pods after they're deployed
    WHERE validator_status IN ('active_ongoing') --other statuses: 'exited_unslashed', 'withdrawal_possible', 'pending_queued', 'pending_initialized', 'withdrawal_done'
    GROUP BY v.day_, v.validator_index
    ORDER BY v.day_, v.validator_index
)

SELECT * FROM DailyRestakedNativeETH