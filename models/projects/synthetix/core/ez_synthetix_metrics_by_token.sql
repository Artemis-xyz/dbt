{{
    config(
        materialized="table",
        snowflake_warehouse="SYNTHETIX",
        database="synthetix",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

treasury_by_token as (
    SELECT
        date,
        token,
        sum(usd_balance) as usd_balance
    FROM {{ ref('fact_synthetix_treasury_by_token') }}
    where usd_balance > 0
    group by 1,2
)
, net_treasury as (
    SELECT
        date,
        token,
        sum(usd_balance) as net_treasury_usd
    FROM {{ ref('fact_synthetix_treasury_by_token') }}
    where token <> 'SNX'
    and usd_balance > 0
    group by 1,2
)
, treasury_native as (
    SELECT
        date,
        token,
        sum(native_balance) as treasury_native
    FROM {{ ref('fact_synthetix_treasury_by_token') }}
    where token = 'SNX'
    and native_balance > 0
    group by 1,2
) 
, date_token_spine as (
    SELECT
        distinct
        date,
        token
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
    treasury_by_token.usd_balance as treasury_value
    , net_treasury.net_treasury_usd as net_treasury_value
    , treasury_native.treasury_native as treasury_native
from date_token_spine
full outer join treasury_by_token using(date, token)
full outer join net_treasury using(date, token)
full outer join treasury_native using(date, token)
