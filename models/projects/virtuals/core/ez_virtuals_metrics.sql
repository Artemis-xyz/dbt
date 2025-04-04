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
        date
        , daily_agents
    from {{ ref("fact_virtuals_daily_agents") }}
),
dau as (
    select
        date
        , dau
    from {{ ref("fact_virtuals_dau") }}
),
volume as (
    select
        date
        , volume_native
        , volume_usd
    from {{ ref("fact_virtuals_volume") }}
)
, fees as (
    select
        date
        , fee_fun_native
        , fee_fun_usd
        , tax_usd
        , fees
    from {{ ref("fact_virtuals_fees") }}
)
, market_data as (
    {{ get_coingecko_metrics('virtual-protocol') }}
)
select
    date
    , coalesce(daily_agents, 0) as daily_agents
    , coalesce(dau, 0) as dau
    , coalesce(volume_native, 0) as volume_native
    , coalesce(volume_usd, 0) as volume_usd
    , coalesce(fee_fun_native, 0) as fee_fun_native
    , coalesce(fee_fun_usd, 0) as fee_fun_usd
    , coalesce(tax_usd, 0) as tax_usd
    , coalesce(fees, 0) as fees

    -- Standardized Metrics

    -- Token Metrics
    , coalesce(price, 0) as price
    , coalesce(market_cap, 0) as market_cap
    , coalesce(fdmc, 0) as fdmc
    , coalesce(token_volume, 0) as token_volume

    -- AI Metrics
    , coalesce(dau, 0) as ai_dau
    , coalesce(fees, 0) as ai_fees
    , coalesce(volume_usd, 0) as ai_volume
    , coalesce(daily_agents, 0) as ai_daily_agents

    -- Cash Flow Metrics
    , coalesce(fee_fun_usd,0)  + coalesce(tax_usd,0) as gross_protocol_revenue
    , coalesce(fee_fun_usd,0) as service_cash_flow
    , coalesce(tax_usd,0) as treasury_cash_flow

    -- Turnover Metrics
    , coalesce(token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(token_turnover_fdv, 0) as token_turnover_fdv
from date_spine
left join daily_agents using (date)
left join dau using (date)
left join volume using (date)
left join fees using (date)
left join market_data using (date)
