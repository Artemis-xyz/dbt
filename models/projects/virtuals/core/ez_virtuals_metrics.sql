{{
    config(
        materialized="table",
        snowflake_warehouse = 'VIRTUALS',
        database = 'VIRTUALS',
        schema = 'core',
        alias = 'ez_metrics'
    )
}}

with date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between '2024-09-10' and to_date(sysdate())
)
, daily_agents as (
    select
        date,
        daily_agents
    from {{ ref("fact_virtuals_daily_agents") }}
),
dau as (
    select
        date,
        dau
    from {{ ref("fact_virtuals_dau") }}
),
volume as (
    select
        date,
        volume_native,
        volume_usd
    from {{ ref("fact_virtuals_volume") }}
)
, fees as (
    select
        date,
        fee_fun_native,
        fee_fun_usd,
        tax_usd,
        fees
    from {{ ref("fact_virtuals_fees") }}
)
select
    date,
    daily_agents,
    dau,
    volume_native,
    volume_usd,
    fee_fun_native,
    fee_fun_usd,
    tax_usd,
    fees
from date_spine
left join daily_agents using (date)
left join dau using (date)
left join volume using (date)
left join fees using (date)
