{{
    config(
        materialized='incremental',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='core',
        alias='ez_metrics',
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with tvl as (
    select
        date,
        sum(tvl_usd) as tvl
    from {{ ref('fact_liquity_tvl') }}
    group by 1
)
, outstanding_supply as (
    select
        date,
        sum(outstanding_supply) as outstanding_supply
    from {{ ref('fact_liquity_outstanding_supply') }}
    group by 1
)
, fees_and_revs as (
    select
        date,
        sum(revenue_usd) as revenue_usd
    from {{ ref('fact_liquity_fees_and_revs') }}
    group by 1
)
, token_incentives as (
    select
        date,
        sum(token_incentives) as token_incentives
    from {{ ref('fact_liquity_token_incentives') }}
    group by 1
)
, treasury as (
    select 
        date
        , sum(treasury) as treasury
        , sum(treasury_native) as treasury_native
        , sum(net_treasury) as net_treasury
        , sum(net_treasury_native) as net_treasury_native
        , sum(own_token_treasury) as own_token_treasury
        , sum(own_token_treasury_native) as own_token_treasury_native
    from {{ ref('ez_liquity_metrics_by_token') }}
    group by 1
)
, token_holders as (
    select
        date,
        token_holder_count
    from {{ ref('fact_liquity_token_holders') }}
)
, market_data as (
    {{ get_coingecko_metrics('liquity') }}
)
, date_spine as (
    select
        date
    from {{ ref('dim_date_spine') }}
    where date between '2021-04-05' and to_date(sysdate())
)

select
    ds.date
    , th.token_holder_count
    , tvl.tvl as net_deposits
    , os.outstanding_supply
    , fr.revenue_usd as fees
    , fr.revenue_usd as revenue
    , ti.token_incentives
    , ti.token_incentives as expenses
    , fr.revenue_usd - ti.token_incentives as earnings
    , t.treasury as treasury_value
    , t.own_token_treasury as treasury_value_native
    , t.net_treasury as net_treasury_value
    -- Standardized Metrics
    -- Token Metrics
    , md.price
    , md.market_cap
    , md.fdmc
    , md.token_volume
    -- Lending Metrics
    , tvl.tvl as lending_deposits
    , fr.revenue_usd as lending_fees
    , os.outstanding_supply as lending_loans
    -- Crypto Metrics
    , tvl.tvl
    , tvl.tvl - lag(tvl.tvl) over (order by date) as tvl_net_change
    -- Cash Flow Metrics
    , fr.revenue_usd as ecosystem_revenue
    , ti.token_incentives as staking_fee_allocation
    -- Protocol Metrics
    , coalesce(t.treasury, 0) as treasury
    , coalesce(t.treasury_native, 0) as treasury_native
    , coalesce(t.net_treasury, 0) as net_treasury
    , coalesce(t.net_treasury_native, 0) as net_treasury_native
    , coalesce(t.own_token_treasury, 0) as own_token_treasury  
    , coalesce(t.own_token_treasury_native, 0) as own_token_treasury_native
    -- Turnover Metrics
    , md.token_turnover_circulating
    , md.token_turnover_fdv
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from date_spine ds
left join tvl using (date)
left join outstanding_supply os using (date)
left join fees_and_revs fr using (date)
left join token_holders th using (date)
left join market_data md using (date)
left join token_incentives ti using (date)
left join treasury t using (date)
where true
{{ ez_metrics_incremental('ds.date', backfill_date) }}
and ds.date < to_date(sysdate())