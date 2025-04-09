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
    -- Daily transaction metrics from inputs
    input_metrics as (
        select
            date_trunc('day', block_timestamp) as date,
            count(distinct transaction_hash) as txns,
            count(distinct block_hash) as blocks,
            sum(value) / 100000000 as volume_native, -- satoshis to LTC
            'litecoin' as chain
        from {{ ref("fact_litecoin_inputs") }}
        group by 1
    ),
    
    -- Block metrics
    block_metrics as (
        select
            date_trunc('day', timestamp) as date,
            count(*) as blocks_mined,
            sum(transaction_count) as total_transactions,
            sum(weight) as total_weight,
            sum(size) as total_size,
            avg(transaction_count) as avg_transactions_per_block
        from {{ ref("fact_litecoin_blocks") }}
        group by 1
    ),
    
    -- Transaction metrics
    transaction_metrics as (
        select
            date_trunc('day', block_timestamp) as date,
            count(*) as total_txns,
            count(distinct transaction_hash) as unique_txns,
            count(distinct block_hash) as blocks_with_txns
        from {{ ref("fact_litecoin_transactions") }}
        group by 1
    ),
    
    -- Calculate active addresses (unique addresses in inputs)
    active_addresses as (
        select
            date_trunc('day', block_timestamp) as date,
            count(distinct f.value:addresses) as daily_active_addresses
        from {{ ref("fact_litecoin_inputs") }}, 
             lateral flatten(input => addresses) f
        group by 1
    ),
    
    -- Calculate rolling metrics (7-day and 30-day)
    rolling_metrics as (
        select
            date,
            sum(daily_active_addresses) over (order by date rows between 6 preceding and current row) as wau,
            sum(daily_active_addresses) over (order by date rows between 29 preceding and current row) as mau
        from active_addresses
    )
    
select
    input_metrics.date,
    input_metrics.chain,
    input_metrics.txns,
    input_metrics.blocks,
    input_metrics.volume_native,
    block_metrics.blocks_mined,
    block_metrics.total_transactions,
    block_metrics.total_weight,
    block_metrics.total_size,
    block_metrics.avg_transactions_per_block,
    transaction_metrics.total_txns,
    transaction_metrics.unique_txns,
    transaction_metrics.blocks_with_txns,
    active_addresses.daily_active_addresses,
    rolling_metrics.wau,
    rolling_metrics.mau
from input_metrics
left join block_metrics on input_metrics.date = block_metrics.date
left join transaction_metrics on input_metrics.date = transaction_metrics.date
left join active_addresses on input_metrics.date = active_addresses.date
left join rolling_metrics on input_metrics.date = rolling_metrics.date
where input_metrics.date < to_date(sysdate()) 