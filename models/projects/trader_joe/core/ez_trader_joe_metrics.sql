{{
    config(
        materialized="table",
        snowflake_warehouse="TRADER_JOE",
        database="trader_joe",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    v2_tvl as (
        with agg as (
            select
                date,
                avg(tvl) as tvl
            from
                pc_dbt_db.prod.fact_defillama_protocol_tvls
            where
                defillama_protocol_id in (
                    select
                        distinct id
                    from
                        pc_dbt_db.prod.fact_defillama_protocols
                    where
                        name in ('Joe V2.1', 'Joe V2.2', 'Joe V2')
                )
            group by date
        )
        SELECT date(date) as date, sum(tvl) as tvl FROM agg
            group by 1 order by 1 desc
    )

, ez_metrics_agg as (    
    select
        date(date) as date,
        app,
        category,
        sum(tvl) as tvl,
        sum(trading_volume) as trading_volume,
        sum(trading_fees) as trading_fees,
        sum(revenue) as revenue,
        sum(unique_traders) as unique_traders,
        sum(gas_cost_native) as gas_cost_native,
        sum(gas_cost_usd) as gas_cost_usd,
        sum(txns) as txns
    from {{ref("ez_trader_joe_metrics_by_chain")}}
    group by 1, 2, 3
)


select
    date(coalesce(v2_tvl.date, e.date)) as date,
    app,
    category,
    sum(coalesce(e.tvl,0) + coalesce(v2_tvl.tvl, 0)) as tvl,
    sum(trading_volume) as trading_volume,
    sum(trading_fees) as trading_fees,
    sum(revenue) as revenue,
    sum(unique_traders) as unique_traders,
    sum(gas_cost_native) as gas_cost_native,
    sum(gas_cost_usd) as gas_cost_usd,
    sum(txns) as txns
from ez_metrics_agg e
left join v2_tvl using (date)
group by 1, 2, 3
order by 1 desc
