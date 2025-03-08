{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BAM_TRANSACTION_SM",
    )
}}

with
    prices as ({{ get_coingecko_price_with_latest("sonic-3") }})
select
    hash_hex as tx_hash,
    coalesce(to_hex, t.from_hex) as contract_address,
    CONVERT_TIMEZONE('UTC', block_time) as block_timestamp,
    block_timestamp::date raw_date,
    t.from_hex as from_address,
    gas_price * gas_used/1E18 as tx_fee,
    (tx_fee * price) gas_usd,
    'sonic' as chain
from zksync_dune.sonic.transactions as t
left join prices on raw_date = prices.date
where 1=1
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run 
        and block_time
        >= (select dateadd('day', -5, max(block_timestamp)) from {{ this }})
    {% endif %}
