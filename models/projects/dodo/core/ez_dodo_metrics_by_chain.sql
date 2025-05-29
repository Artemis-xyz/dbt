{{
    config(
        materialized="table",
        snowflake_warehouse="DODO",
        database="dodo",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with swap_metrics as (
    select
        block_timestamp::date as date,
        chain,
        count(distinct tx_hash) as txns,
        count(distinct swapper) as dau,
        sum(amount_usd) as volume_usd
    from {{ref('fact_dodo_swap_metrics')}}
    group by date, chain
)
, token_incentives as (
    select
        block_timestamp::date as date,
        'ethereum' as chain,
        sum(amount_usd) as token_incentives
    from {{ref('fact_dodo_token_incentives')}}
    group by date, chain
)
, date_spine as (
    select * 
    from {{ ref('dim_date_spine') }}
    where date between (select min(date) from swap_metrics) and to_date(sysdate())
)
, chains as (
    select distinct chain
    from swap_metrics
)
, date_chain_spine as (
    select 
        date_spine.date,
        chains.chain
    from date_spine
    cross join chains
)

select
    date_chain_spine.date,
    date_chain_spine.chain

    -- Standardized Metrics
    
    -- Usage Metrics
    , coalesce(swap_metrics.txns, 0) as spot_txns
    , coalesce(swap_metrics.dau, 0) as spot_dau
    --, coalesce(swap_metrics.volume_usd, 0) as spot_volume

    -- Cashflow Metrics
    , coalesce(token_incentives.token_incentives, 0) as token_incentives

from date_chain_spine
left join swap_metrics using (date, chain)
left join token_incentives using (date, chain)
where date_chain_spine.date <= to_date(sysdate())