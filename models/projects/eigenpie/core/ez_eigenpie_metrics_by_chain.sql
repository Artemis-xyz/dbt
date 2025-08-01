{{
    config(
        materialized="table",
        snowflake_warehouse="EIGENPIE",
        database="eigenpie",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with restaked_eth_metrics as (
    select
        date,
        chain,
        num_restaked_eth,
        amount_restaked_usd,
        num_restaked_eth_net_change,
        amount_restaked_usd_net_change
    from {{ ref('fact_eigenpie_restaked_eth_count_with_usd_and_change') }}
),
date_spine as (
    select
        ds.date
    from {{ ref('dim_date_spine') }} ds
    where ds.date between (select min(date) from restaked_eth_metrics) and to_date(sysdate())
)

select
    date_spine.date,
    'eigenpie' as artemis_id,
    restaked_eth_metrics.chain,

    -- Standardized Metrics
    , restaked_eth_metrics.num_restaked_eth as lrt_tvl_native
    , restaked_eth_metrics.amount_restaked_usd as lrt_tvl
    , restaked_eth_metrics.amount_restaked_usd as tvl
    , restaked_eth_metrics.num_restaked_eth as tvl_native


from date_spine
left join restaked_eth_metrics using(date)
where date_spine.date < to_date(sysdate())
