{{
    config(
        materialized="table",
        snowflake_warehouse="LITECOIN",
        database="litecoin",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    github_data as ({{ get_github_metrics("litecoin") }}),
    price_data as ({{ get_coingecko_metrics("litecoin") }}),
    
    -- Daily transaction metrics from transactions
    transaction_metrics as (
        select
            date_trunc('day', block_timestamp) as date,
            count(*) as txns,
            sum(fee) / 100000000 as fees_native, -- Convert satoshis to LTC
            sum(fee) / 100000000 * price_data.price as fees, -- Use actual price
            avg(fee) / 100000000 as avg_txn_fee,
            sum(fee) / 100000000 * price_data.price as revenue, -- Use actual price
            'litecoin' as chain
        from {{ ref("fact_litecoin_transactions") }}
        left join price_data on date_trunc('day', block_timestamp) = price_data.date
        group by 1
    ),
    
    -- Block metrics and issuance
    block_metrics as (
        select
            date_trunc('day', timestamp) as date,
            count(*) as blocks_mined,
            sum(transaction_count) as total_transactions,
            sum(weight) as total_weight,
            sum(size) as total_size,
            avg(transaction_count) as avg_transactions_per_block,
            sum(case 
                when number < 840000 then 50
                when number < 1680000 then 25
                when number < 2520000 then 12.5
                when number < 3360000 then 6.25
                when number < 4200000 then 3.125
                else 1.5625
            end) as issuance
        from {{ ref("fact_litecoin_blocks") }}
        group by 1
    ),
    
    -- Calculate circulating supply
    supply_metrics as (
        select
            date_trunc('day', timestamp) as date,
            sum(case 
                when number < 840000 then 50
                when number < 1680000 then 25
                when number < 2520000 then 12.5
                when number < 3360000 then 6.25
                when number < 4200000 then 3.125
                else 1.5625
            end) as circulating_supply
        from {{ ref("fact_litecoin_blocks") }}
        group by 1
    ),
    
    -- Calculate active addresses (unique addresses in inputs)
    active_addresses as (
        select
            date_trunc('day', block_timestamp) as date,
            count(distinct f.value) as dau
        from {{ ref("fact_litecoin_inputs") }}, 
             lateral flatten(input => addresses) f
        group by 1
    ),
    
    -- Calculate rolling metrics (7-day and 30-day)
    rolling_metrics as (
        select
            date,
            sum(dau) over (order by date rows between 6 preceding and current row) as wau,
            sum(dau) over (order by date rows between 29 preceding and current row) as mau
        from active_addresses
    )
    
select
    transaction_metrics.date,
    transaction_metrics.chain,
    transaction_metrics.txns,
    active_addresses.dau,
    rolling_metrics.wau,
    rolling_metrics.mau,
    transaction_metrics.fees_native,
    transaction_metrics.fees,
    transaction_metrics.avg_txn_fee,
    transaction_metrics.revenue,
    block_metrics.issuance,
    supply_metrics.circulating_supply,
    price_data.price,
    price_data.price * supply_metrics.circulating_supply as market_cap,
    price_data.price * supply_metrics.circulating_supply as fdmc
from transaction_metrics
left join block_metrics on transaction_metrics.date = block_metrics.date
left join supply_metrics on transaction_metrics.date = supply_metrics.date
left join active_addresses on transaction_metrics.date = active_addresses.date
left join rolling_metrics on transaction_metrics.date = rolling_metrics.date
left join price_data on transaction_metrics.date = price_data.date
where transaction_metrics.date < to_date(sysdate()) 