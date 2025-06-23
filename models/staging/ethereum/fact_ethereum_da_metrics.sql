{{
    config(
        materialized="table",
    )
}}
with
    prices as ({{get_coingecko_price_with_latest("ethereum")}}),
    blob_transactions as (
        select
            block_timestamp::date as date,
            from_address as submitter,
            (blob_gas_used * blob_gas_price) as blob_fees_gas,
            blob_fees_gas / 1e9 as blob_fees_native,
            blob_fees_native * price as blob_fees
        from ethereum_flipside.core.fact_transactions 
        left join prices on block_timestamp::date = prices.date
        where tx_type = '3'
    ),
    blob_gas_fees as (
        select 
            date,
            sum(blob_fees_gas) as blob_fees_gas,
            sum(blob_fees_native) as blob_fees_native,
            sum(blob_fees) as blob_fees,
            count(distinct submitter) as submitters
        from blob_transactions
        group by date
    ),
    beacon_chain_blob_data as (
        select 
            slot_timestamp::date as date,
            sum((len(blob) - 2) / 2) as bytes,
        from ethereum_flipside.beacon_chain.fact_blob_sidecars 
        left join ethereum_flipside.beacon_chain.fact_blocks using(slot_number)
        group by date
    )
select
    date,
    'ethereum' as chain,
    blob_fees_native,
    blob_fees,
    (bytes/pow(1024, 2)) as blob_size_mib,
    (bytes / pow(1024, 2)) / 86400 as avg_mib_per_second,
    (blob_fees_gas /0.001048576) / (bytes) as avg_cost_per_mib_gwei,
    ((blob_fees_gas /1.048576E12) * price) / (bytes) as avg_cost_per_mib,
    submitters
from blob_gas_fees
left join beacon_chain_blob_data using(date)
left join prices using(date)
where date < to_date(sysdate())
