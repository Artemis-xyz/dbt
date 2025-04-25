{{
    config(
        materialized='incremental',
        unique_key= ['tx_hash', 'event_index'],
        snowflake_warehouse='MAPLE'
    )
}}

with pools as (
    SELECT
        decoded_log:pool as pool_address,
        decoded_log:liquidityAsset as pool_liquidity_asset
    FROM
        {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }}
    WHERE CONTRACT_ADDRESS = lower('0x2Cd79F7f8b38B9c0D80EA6B230441841A31537eC')
    AND event_name = 'PoolCreated'
)

select
    l.block_timestamp,
    l.tx_hash,
    l.event_index,
    l.contract_address,
    l.topics,
    l.data,
    p.pool_liquidity_asset,
    PC_DBT_DB.PROD.HEX_TO_INT(substr(l.data, 0, 64+2)) as interest,
    PC_DBT_DB.PROD.HEX_TO_INT(substr(l.data, 64+3, 64)) as principal,
    PC_DBT_DB.PROD.HEX_TO_INT(substr(l.data, 128+3, 64)) as fee
from {{ source('ETHEREUM_FLIPSIDE', 'fact_event_logs') }} l
join pools p on p.pool_address = l.contract_address
where topics[0] = lower('0x21280d282ce6aa29c649fd1825373d7c77892fac3f1958fd98d5ca52dd82a197')
{% if is_incremental() %}
    AND l.block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %}