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
, v2_data as (
    select
        date,
        sum(total_volume) as total_volume,
        sum(total_fees) as total_fees,
        sum(protocol_fees) as protocol_fees,
        sum(unique_traders) as unique_traders,
        sum(daily_txns) as daily_txns
    from {{ ref("fact_trader_joe_v2_all_versions_metrics")}}
    group by 1
)
, ez_metrics_agg as (    
    select
        date(date) as date,
        app,
        category,
        sum(tvl) as tvl,
        sum(trading_volume) as trading_volume,
        sum(trading_fees) as trading_fees,
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
    sum(coalesce(e.trading_volume, 0) + coalesce(v2_data.total_volume, 0))   as trading_volume,
    sum(coalesce(e.trading_fees, 0) + coalesce(v2_data.total_fees, 0)) as trading_fees,
    sum(coalesce(v2_data.protocol_fees, 0)) as revenue,
    sum(coalesce(e.unique_traders, 0) + coalesce(v2_data.unique_traders, 0)) as unique_traders,
    sum(coalesce(e.gas_cost_native, 0)) as gas_cost_native,
    sum(coalesce(e.gas_cost_usd, 0)) as gas_cost_usd,
    sum(coalesce(e.txns, 0) + coalesce(v2_data.daily_txns, 0)) as txns
from ez_metrics_agg e
left join v2_data using (date)
left join v2_tvl using (date)
group by 1, 2, 3
order by 1 desc
