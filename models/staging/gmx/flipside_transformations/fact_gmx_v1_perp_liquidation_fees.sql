{{ config(materialized="table", snowflake_warehouse="GMX") }}

with arbitrum_trades as ( 
    select
        date_trunc('day', block_timestamp) as date,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        decoded_log,
        CONCAT(tx_hash, '-', event_index) as log_id,
        decoded_log:collateralDelta::NUMBER / 1e30 as collateralDelta,
        decoded_log:collateralToken::STRING  as collateral_token,
        decoded_log:fee::NUMBER / 1e30 as fees,
        decoded_log:sizeDelta::NUMBER / 1e30 as volume,
        decoded_log:account::STRING as trader
    from arbitrum_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x489ee077994B6658eAfA855C308275EAd8097C4A')
    and event_name = 'IncreasePosition' 

    union all 

    select
        date_trunc('day', block_timestamp) as date,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        decoded_log,
        CONCAT(tx_hash, '-', event_index) as log_id,
        decoded_log:collateralDelta::NUMBER / 1e30 as collateralDelta,
        decoded_log:collateralToken::STRING as collateral_token, 
        decoded_log:fee::NUMBER / 1e30 as fees,
        decoded_log:sizeDelta::NUMBER / 1e30 as volume,
        decoded_log:account::STRING as trader
    from arbitrum_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x489ee077994B6658eAfA855C308275EAd8097C4A')
    and event_name = 'DecreasePosition'
),
arbitrum_liquidations as (
    SELECT
        -- Distinct Trader Count is 0. I count it above        
        block_timestamp
        , DATE_TRUNC('day',block_timestamp) AS date
        , decoded_log:account::STRING as trader
        , decoded_log:size::NUMBER / 1e30 AS volume
        , 0 AS fees
    FROM arbitrum_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x489ee077994B6658eAfA855C308275EAd8097C4A')
    and event_name = 'LiquidatePosition'
    
    UNION ALL
    
    SELECT 
        block_timestamp
        , DATE_TRUNC('day',block_timestamp) AS date
        , NULL AS trader
        , 0 AS volume
        , decoded_log:feeUsd::NUMBER / 1e30  AS fees
    FROM arbitrum_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x489ee077994B6658eAfA855C308275EAd8097C4A')
    and event_name = 'CollectMarginFees'
    
    UNION ALL
    
    SELECT 
          block_timestamp
        , date
        , NULL AS trader
        , 0 AS volume
        , -(fees) AS fees
    FROM arbitrum_trades
)

--AVALANCHE
, avalanche_trades as ( 
    select
        date_trunc('day', block_timestamp) as date,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        decoded_log,
        CONCAT(tx_hash, '-', event_index) as log_id,
        decoded_log:collateralDelta::NUMBER / 1e30 as collateralDelta,
        decoded_log:collateralToken::STRING  as collateral_token,
        decoded_log:fee::NUMBER / 1e30 as fees,
        decoded_log:sizeDelta::NUMBER / 1e30 as volume,
        decoded_log:account::STRING as trader
    from avalanche_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x9ab2de34a33fb459b538c43f251eb825645e8595')
    and event_name = 'IncreasePosition' 

    union all 

    select
        date_trunc('day', block_timestamp) as date,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        decoded_log,
        CONCAT(tx_hash, '-', event_index) as log_id,
        decoded_log:collateralDelta::NUMBER / 1e30 as collateralDelta,
        decoded_log:collateralToken::STRING as collateral_token, 
        decoded_log:fee::NUMBER / 1e30 as fees,
        decoded_log:sizeDelta::NUMBER / 1e30 as volume,
        decoded_log:account::STRING as trader
    from avalanche_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x9ab2de34a33fb459b538c43f251eb825645e8595')
    and event_name = 'DecreasePosition'
),
avalanche_liquidations as (
    SELECT
        -- Distinct Trader Count is 0. I count it above        
        block_timestamp
        , DATE_TRUNC('day',block_timestamp) AS date
        , decoded_log:account::STRING as trader
        , decoded_log:size::NUMBER / 1e30 AS volume
        , 0 AS fees
    FROM avalanche_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x9ab2de34a33fb459b538c43f251eb825645e8595')
    and event_name = 'LiquidatePosition'
    
    UNION ALL
    
    SELECT 
        block_timestamp
        , DATE_TRUNC('day',block_timestamp) AS date
        , NULL AS trader
        , 0 AS volume
        , decoded_log:feeUsd::NUMBER / 1e30  AS fees
    FROM avalanche_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x9ab2de34a33fb459b538c43f251eb825645e8595')
    and event_name = 'CollectMarginFees'
    
    UNION ALL
    
    SELECT 
          block_timestamp
        , date
        , NULL AS trader
        , 0 AS volume
        , -(fees) AS fees
    FROM avalanche_trades
),
liquidations as (
    SELECT 
         date
        , 'arbitrum' as chain
        , 'v1' as version
        , sum(fees) as fees
    FROM arbitrum_liquidations
    group by date

    UNION ALL
    
    SELECT 
         date
        , 'avalanche' as chain
        , 'v1' as version
        , sum(fees) as fees
    from avalanche_liquidations
    group by date
)
select * from liquidations