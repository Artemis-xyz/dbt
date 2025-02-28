{{
    config(
        materialized='table',
        snowflake_warehouse='CONVEX',
        database='CONVEX',
        schema='raw',
        alias='fact_convex_revenue'
    )
}}

with transfers as (
    select
        block_timestamp::date as date,
        contract_address,
        sum(RAW_AMOUNT_PRECISE) as claimed
    from
        {{ source('ETHEREUM_FLIPSIDE', 'fact_token_transfers') }}
    where
        contract_address = lower('0xD533a949740bb3306d119CC777fa900bA034cd52')
        and from_address = lower('0x0000000000000000000000000000000000000000')
        and to_address = lower('0x989aeb4d175e16225e39e87d0d97a3360524ad80')
    group by
        1,
        2
    union all
    select
        block_timestamp::date as date,
        contract_address,
        sum(RAW_AMOUNT_PRECISE) as claimed
    from
        {{ source('ETHEREUM_FLIPSIDE', 'fact_token_transfers') }}
    where
        contract_address = lower('0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490')
        and to_address = lower('0x7091dbb7fcbA54569eF1387Ac89Eb2a5C9F6d2EA')
    group by
        1,
        2
),
date_address_spine AS (
    SELECT
        distinct ds.date,
        t.contract_address
    FROM
        {{ ref('dim_date_spine') }} ds
        JOIN transfers t
    WHERE
        ds.date between (
            select
                min(date)
            from
                transfers
        )
        and to_date(sysdate())
)
select
    das.date,
    p.symbol as token,
    'ethereum' as chain,
    sum((t.claimed / power(10, p.decimals)) * p.price) as fees,
    fees * 0.17 as revenue,
    fees * 0.83 as primary_supply_side_fees
from
    date_address_spine as das
    left join transfers as t on (
        t.date = das.date
        and t.contract_address = das.contract_address
    )
    left join {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p on (
        p.token_address = t.contract_address
        and p.hour = das.date
    )
group by
    1,
    2
HAVING
    fees is not null