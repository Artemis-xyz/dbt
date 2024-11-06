{{
    config(
        materialized="incremental",
        unique_key= ['tx_hash', 'event_index'],
        snowflake_warehouse="MAPLE",
    )
}}

-- Works but off by 8 txs

with pools as (
    SELECT * FROM {{ ref('dim_maple_v2_pool_deposit_pools') }}
)
SELECT 
    block_timestamp
    , tx_hash
    , event_index
    , block_number as block
    , contract_address
    , decoded_log:assets_::number as assets_
    , decoded_log:caller_::string as caller_
    , decoded_log:owner_::string as owner_
    , decoded_log:shares_::number as shares_
FROM
    {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}} l
join pools p on lower(p.pool_address) = lower(l.contract_address)
where event_name = 'Deposit'
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %}
