{{ config(materialized="table", snowflake_warehouse="AAVE") }}
with
    unioned as (
        select *
        from landing_database.prod_landing.raw_aave_v2_lending_ethereum
        union all
        select *
        from
            {{
                source(
                    "PROD_LANDING", "raw_aave_v2_ethereum_borrows_deposits_revenue"
                )
            }}

    ),
    dates as (
        select
            extraction_date,
            to_timestamp(trunc(flat_json.value:"day"::timestamp, 'day')) as date
        from unioned t1, lateral flatten(input => parse_json(source_json)) as flat_json
        group by date, extraction_date
    ),
    max_extraction_per_day as (
        select date, max(extraction_date) as extraction_date
        from dates
        group by date
        order by date
    ),
    flattened_json as (
        select
            extraction_date,
            to_timestamp(trunc(flat_json.value:"day"::timestamp, 'day')) as date,
            flat_json.value:"category"::string as category,
            flat_json.value:"app"::string as app,
            flat_json.value:"underlying_token"::string as underlying_token,
            flat_json.value:"underlying_token_price"::float as underlying_token_price,
            flat_json.value:"stable_borrows"::float as stable_borrows,
            flat_json.value:"variable_borrows"::float as variable_borrows,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    ),
    map_reduce_json as (
        select t1.*
        from flattened_json t1
        left join max_extraction_per_day t2 on t1.date = t2.date
        where t1.extraction_date = t2.extraction_date
    ),
    combined_data as (
        select
            date,
            'ethereum' as chain,
            'aave' as app,
            'DeFi' as category,
            sum(borrows_usd) as daily_borrows_usd,
            sum(supply_usd) as daily_supply_usd
        from map_reduce_json
        group by date
    )

select date, chain, app, category, daily_borrows_usd, daily_supply_usd
from combined_data
