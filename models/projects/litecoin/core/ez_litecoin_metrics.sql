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
            avg(fee) / 100000000 as avg_txn_fee,
            0 as revenue_native, -- Revenue in LTC
            'litecoin' as chain
        from {{ ref("fact_litecoin_transactions") }}
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
    , supply_metrics as (
        SELECT
            date,
            max_supply,
            uncreated_tokens,
            total_supply,
            issued_supply,
            circulating_supply
        FROM {{ ref("fact_litecoin_supply") }}
    )
    
select
    transaction_metrics.date,
    transaction_metrics.chain,
    transaction_metrics.txns,
    active_addresses.dau,
    rolling_metrics.wau,
    rolling_metrics.mau,
    transaction_metrics.fees_native,
    transaction_metrics.fees_native * price_data.price as fees,
    transaction_metrics.avg_txn_fee,
    0 as revenue_native,
    0 as revenue,
    block_metrics.issuance,
    supply_metrics.max_supply as max_supply_native,
    supply_metrics.uncreated_tokens as uncreated_tokens,
    supply_metrics.total_supply as total_supply_native,
    supply_metrics.circulating_supply as circulating_supply_native,
    supply_metrics.issued_supply as issued_supply_native,
    github_data.weekly_commits_core_ecosystem,
    github_data.weekly_commits_sub_ecosystem,
    github_data.weekly_developers_core_ecosystem,
    github_data.weekly_developers_sub_ecosystem,
    price_data.price,
    price_data.price * supply_metrics.circulating_supply as market_cap,
    price_data.price * supply_metrics.circulating_supply as fdmc
from transaction_metrics
left join block_metrics on transaction_metrics.date = block_metrics.date
left join supply_metrics on transaction_metrics.date = supply_metrics.date
left join active_addresses on transaction_metrics.date = active_addresses.date
left join rolling_metrics on transaction_metrics.date = rolling_metrics.date
left join price_data on transaction_metrics.date = price_data.date
left join github_data on transaction_metrics.date = github_data.date
where transaction_metrics.date < to_date(sysdate()) 