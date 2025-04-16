{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BAM_TRANSACTION_SM",
    )
}}

with
    prices as (
        with sonic_price as (
            {{ get_coingecko_price_with_latest("sonic-3") }}
        ),
        ftm_price as (
            {{ get_coingecko_price_with_latest("fantom") }}
        )
        SELECT
            date,
            price
        FROM sonic_price
        WHERE date > '2025-01-02'
        UNION ALL
        SELECT
            date,
            price
        FROM ftm_price
        WHERE date between '2024-12-01' and '2025-01-03' -- sonic (S) price is not available for this period
    )
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
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
