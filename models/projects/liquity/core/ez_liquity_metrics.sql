{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='core',
        alias='ez_metrics'
    )
}}

with tvl as (
    select
        date,
        tvl_usd as tvl
    from {{ ref('fact_liquity_tvl') }}
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
    SELECT
        date,
        sum(native_balance) as treasury_value,
        SUM(
            CASE WHEN token = 'LQTY'
                THEN native_balance
            END
        ) AS treasury_value_native,
        SUM(
            CASE WHEN token <> 'LQTY'
                THEN native_balance
            END
        ) AS net_treasury_value
    FROM {{ ref('fact_liquity_treasury') }}
    GROUP BY 1
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
    ds.date,
    
    -- Fees, Revenue, Earnings
    fr.revenue_usd as fees,
    fr.revenue_usd as revenue,
    ti.token_incentives,
    fr.revenue_usd - ti.token_incentives as protocol_earnings,

    -- TVL
    tvl.tvl,
    tvl.tvl as net_deposits,
    os.outstanding_supply,

    -- Treasury
    t.net_treasury_value as net_treasury_value,
    t.treasury_value_native as treasury_native_value,
    t.treasury_value as treasury_value,

    -- Market Data
    md.price,
    md.market_cap,
    md.fdmc,
    md.token_turnover_circulating,
    md.token_turnover_fdv,
    md.token_volume,
    th.token_holder_count
from date_spine ds
left join tvl using (date)
left join outstanding_supply os using (date)
left join fees_and_revs fr using (date)
left join token_holders th using (date)
left join market_data md using (date)
left join token_incentives ti using (date)
left join treasury t using (date)