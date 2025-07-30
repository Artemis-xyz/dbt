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
    ,'liquity' as artemis_id

    --Market Data
    , md.price
    , md.market_cap as mc
    , md.fdmc
    , md.token_volume

    --Usage Data
    , th.token_holder_count
    , tvl.tvl
    , tvl.tvl as lending_deposits
    , os.outstanding_supply as lending_loans

    --Fee Data
    , fr.revenue_usd / price as fees_native
    , fr.revenue_usd as lending_fees
    , fr.revenue_usd as fees

    --Fee Allocation
    , ti.token_incentives as staking_fee_allocation

    --Financial Statements
    , fr.revenue_usd / price as revenue_native
    , fr.revenue_usd as revenue
    , ti.token_incentives
    , ti.token_incentives as expenses
    , fr.revenue_usd - ti.token_incentives as earnings

    --Treasury Data
    , coalesce(t.treasury_native, 0) as treasury_native
    , coalesce(t.treasury, 0) as treasury
    , coalesce(t.net_treasury_native, 0) as net_treasury_native
    , coalesce(t.net_treasury, 0) as net_treasury
    , coalesce(t.own_token_treasury_native, 0) as own_token_treasury_native
    , coalesce(t.own_token_treasury, 0) as own_token_treasury  

    --Token Turnover/Other Data
    , md.token_turnover_fdv
    , md.token_turnover_circulating
    
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
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