{{
    config(
        materialized='table',
        snowflake_warehouse='CONVEX',
        database='CONVEX',
        schema='raw',
        alias='fact_convex_cvxcrv_balance'
    )
}}

WITH dates as (
    select
        extraction_date,
        to_timestamp(trunc(flat_json.value:"date"::timestamp, 'day')) as date
    from
        {{ source("PROD_LANDING", "raw_convex_staker_cvxcrv") }} t1,
        lateral flatten(input => parse_json(source_json)) as flat_json
    group by
        date,
        extraction_date
),
max_extraction_per_day as (
    select
        date,
        max(extraction_date) as extraction_date
    from
        dates
    group by
        date
    order by
        date
),
flattened_json as (
    select
        extraction_date,
        flat_json.value:date::date as date,
        flat_json.value:contract_address::string as contract_address,
        flat_json.value:vecrv_balance::number as balance
    from
        {{ source('PROD_LANDING', 'raw_convex_staker_cvxcrv') }} t1,
        lateral flatten(input => parse_json(source_json)) as flat_json
)
select
    t1.date,
    t1.contract_address,
    t1.balance
from
    flattened_json t1
    left join max_extraction_per_day t2 on t1.date = t2.date
where
    t1.extraction_date = t2.extraction_date