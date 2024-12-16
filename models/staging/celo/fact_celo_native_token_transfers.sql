{{ 
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "index"],
        snowflake_warehouse="CELO_LG"
    )
}}
with
prices as ({{get_coingecko_price_with_latest('celo')}})
, celo_native_transfers as (
    select
        t1.block_timestamp,
        t1.block_number,
        t1.transaction_hash,
        trace_address as index,
        t2.from_address as origin_from_address,
        t2.to_address as origin_to_address,
        'native-token:42220' as contract_address,
        t1.from_address,
        t1.to_address,
        t1.value as amount,
        t1.status as tx_status
    from {{ ref("fact_celo_traces") }} t1
    left join {{ ref("fact_celo_transactions") }} t2 using(transaction_hash)
    where t1.status = 1
    {% if is_incremental() %}
        and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
    union
    select distinct
        t1.block_timestamp,
        t1.block_number,
        t1.transaction_hash,
        '-1' as index,
        t1.from_address as origin_from_address,
        t1.to_address as origin_to_address,
        'native-token:42220' as contract_address,
        t1.from_address,
        t2.miner as to_address,
        t1.gas * t1.gas_price as amount,
        1 as tx_status -- Fees are paid whether or not the transaction is successful
    from {{ref("fact_celo_transactions")}} t1
    left join {{ref("fact_celo_blocks")}} t2 using (block_number)
    where fee_currency is null 
    {% if is_incremental() %}
        and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
)
select
    block_timestamp,
    block_number,
    transaction_hash,
    index,
    origin_from_address,
    origin_to_address,
    contract_address,
    from_address,
    to_address,
    amount,
    amount / pow(10, 18) as amount_adjusted,
    amount_adjusted * price as amount_usd,
    tx_status
from celo_native_transfers
left join prices on block_timestamp::date = prices.date
where amount is not null and amount > 0

