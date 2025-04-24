{{
    config(
        materialized="table",
        database = 'ondo',
        schema = 'core',
        snowflake_warehouse = 'ONDO',
        alias = 'ez_metrics'
    )
}}

with date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date between '2023-01-26' and to_date(sysdate())
)

, fees as (
    select date, fee as fees from {{ ref("fact_ondo_ousg_fees") }}
)
, tvl as (
    select
        date,
        sum(tokenized_mcap_change) as tokenized_mcap_change,
        sum(tokenized_mcap) as tokenized_mcap,
    from {{ ref("ez_ondo_metrics_by_chain") }}
    group by 1
)
, ff_defillama_metrics as (
    select
        date,
        tvl
    from {{ ref("fact_defillama_protocol_tvls") }}
    where defillama_protocol_id = 2537
)

, supply as (
    select
        date,
        premine_unlocks_native,
        net_supply_change_native,
        circulating_supply_native
    from {{ ref("fact_ondo_daily_supply") }}
)

select
    ds.date,
    coalesce(fees.fees, 0) as fees,
    coalesce(tvl.tokenized_mcap_change, 0) as tokenized_mcap_change,
    coalesce(tvl.tokenized_mcap, 0) as tokenized_mcap,
    coalesce(ff_defillama_metrics.tvl, 0) as flux_finance_tvl,
    coalesce(supply.premine_unlocks_native, 0) as premine_unlocks_native,
    coalesce(supply.net_supply_change_native, 0) as net_supply_change_native,
    coalesce(supply.circulating_supply_native, 0) as circulating_supply_native
from date_spine ds
left join fees using (date)
left join tvl using (date)
left join ff_defillama_metrics using (date)
left join supply using (date)
where ds.date < to_date(sysdate())