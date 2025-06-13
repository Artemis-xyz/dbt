{{
    config(
        materialized="incremental",
        snowflake_warehouse="ANALYTICS_XL",
        unique_key=["date", "incentive_type"],
    )
}}

--QUERY

-- Takes 12 min to run 2 month historic data on warehouse ANALYTICS_XL
with block_rewards as (
    select
        b.timestamp,
        b.timestamp::date as date,
        b.number,
        b.hash,
        b.miner,
        b.gas_used,
        16 as block_rewards_trx,
        160 as voting_rewards_trx,
        MAX(t.usd_exchange_rate) as max_usd_exchange_rate,
        b.gas_used / POW(10, 6) * MAX(t.usd_exchange_rate) as block_creation_fee,
        block_rewards_trx * AVG(t.usd_exchange_rate) as block_rewards --https://www.findas.org/tokenomics-review/coins/the-tokenomics-of-tron-trx/r/3cDzc5MHjcKZKSnt6hkBje
    from {{ source('TRON_ALLIUM', 'blocks') }} b
    left join {{ source('TRON_ALLIUM_ASSETS', 'trx_token_transfers') }} t
        on DATE_TRUNC('hour', t.block_timestamp) = DATE_TRUNC('hour', b.timestamp)
    {% if is_incremental() %}
        where b.timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% endif %}
    group by b.number, b.timestamp, b.hash, b.miner, b.gas_used
)
, daily_trx_price as (
    select
        date_trunc('hour', block_timestamp) as hour,
        AVG(usd_exchange_rate) as price
    from {{ source('TRON_ALLIUM_ASSETS', 'trx_token_transfers') }}
    {% if is_incremental() %}
        where block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% endif %}
    group by hour
)
, bandwidth_fees as (
    select
        tx.block_timestamp as block_timestamp,
        tx.block_timestamp::date as date,
        tx.hash,
        tx.gas,
        tx.gas_price,
        (tx.gas * tx.gas_price) / 1e6  as transaction_fee,
        ((tx.gas * tx.gas_price) / 1e6) * (t.price)  as transaction_fee_usd,
    from {{ source('TRON_ALLIUM', 'transactions') }} tx
    left join daily_trx_price t
        on t.hour = DATE_TRUNC('hour', tx.block_timestamp)
    {% if is_incremental() %}
        where tx.block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% endif %}
)  

select
    date,
    'bandwidth_fees' as incentive_type,
    sum(transaction_fee_usd) as token_incentives
from bandwidth_fees

group by date

union all 

select
    date,
    'block_rewards' as incentive_type,
    sum(block_rewards)
from block_rewards
group by date
