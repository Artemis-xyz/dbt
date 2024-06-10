{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM",
    )
}}
with
    prices as ({{get_coingecko_price_with_latest("ethereum")}}),
    blob_transactions as (
        select
            block_timestamp::date as date,
            from_address as submitter,
            (blob_gas_used * blob_gas_price) as blob_fee_gas,
            blob_fee_gas / 1e18 as blob_fee_native,
            blob_fee_native * price as blob_fee_usd
        from ethereum_flipside.core.fact_transactions 
        left join prices on block_timestamp::date = prices.date
        where tx_type = '3'
    ),
    blob_gas_fees as (
        select 
            date,
            sum(blob_fee_gas) as blob_fee_gas,
            sum(blob_fee_native) as blob_fee_native,
            sum(blob_fee_usd) as blob_fee_usd,
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
    blob_fee_native,
    blob_fee_usd,
    (bytes/pow(1024, 2)) as blob_size_mib,
    (bytes / pow(1024, 2)) / 86400 as avg_mib_per_second,
    (blob_fee_gas /0.001048576) / (bytes) as avg_gwei_per_mib,
    ((blob_fee_gas /1.048576E12) * price) / (bytes) as avg_usd_per_mib,
    submitters
from blob_gas_fees
left join beacon_chain_blob_data using(date)
left join prices using(date)
where date < to_date(sysdate())
