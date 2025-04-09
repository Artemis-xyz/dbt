{{
    config(
        materialized="table",
        snowflake_warehouse="SYNTHETIX",
        database="synthetix",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

with
    treasury_by_token as (
        SELECT
            date
            , token
        , sum(usd_balance) as treasury
        , sum(native_balance) as treasury_native
    FROM {{ ref('fact_synthetix_treasury_by_token') }}
    where usd_balance > 0
    group by 1,2
    )
    , net_treasury as (
        SELECT
            date
            , token
            , sum(usd_balance) as net_treasury
            , sum(native_balance) as net_treasury_native
        FROM {{ ref('fact_synthetix_treasury_by_token') }}
        where token <> 'SNX'
        and usd_balance > 0
        group by 1,2
    )
    , treasury_native as (
        SELECT
            date
            , token
            , sum(usd_balance) as own_token_treasury
            , sum(native_balance) as own_token_treasury_native
        FROM {{ ref('fact_synthetix_treasury_by_token') }}
        where token = 'SNX'
        and native_balance > 0
        group by 1,2
    ) 
    , tvl as (
        select
            date,
            token,
            sum(tvl_usd) as tvl
        from {{ ref('fact_synthetix_tvl_by_chain_and_token') }}
        group by 1,2
    )
    , date_token_spine as (
        SELECT
            distinct
            date
            , token
        from {{ ref('dim_date_spine') }}
        CROSS JOIN (
                    SELECT distinct token from treasury_by_token
                    UNION
                    SELECT distinct token from net_treasury
                    UNION
                    SELECT distinct token from treasury_native
                    UNION
                    SELECT distinct token from tvl
                    )
        where date between '2019-08-11' and to_date(sysdate())
    )

select
    date
    , token

    -- Standardized Metrics

    -- Crypto Metrics
    , coalesce(tvl.tvl, 0) as tvl

    -- Protocol Metrics
    , coalesce(treasury_by_token.treasury, 0) as treasury
    , coalesce(treasury_by_token.treasury_native, 0) as treasury_native
    , coalesce(net_treasury.net_treasury, 0) as net_treasury
    , coalesce(net_treasury.net_treasury_native, 0) as net_treasury_native
    , coalesce(treasury_native.own_token_treasury, 0) as own_token_treasury
    , coalesce(treasury_native.own_token_treasury_native, 0) as own_token_treasury_native
from date_token_spine
full outer join treasury_by_token using(date, token)
full outer join net_treasury using(date, token)
full outer join treasury_native using(date, token)
full outer join tvl using(date, token)
