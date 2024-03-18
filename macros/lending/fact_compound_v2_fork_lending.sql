{% macro fact_compound_v2_fork_lending(source_table, chain, app) %}
    with
        dates as (
            select
                extraction_date,
                to_timestamp(trunc(flat_json.value:"day"::timestamp, 'day')) as date
            from
                {{ source("PROD_LANDING", source_table) }} t1,
                lateral flatten(input => parse_json(source_json)) as flat_json
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
                flat_json.value:"underlying_token_price"::float
                as underlying_token_price,
                flat_json.value:"underlying_borrows"::float as underlying_borrows,
                flat_json.value:"borrows_usd"::float as borrows_usd,
                flat_json.value:"underlying_supply"::float as underlying_supply,
                flat_json.value:"supply_usd"::float as supply_usd
            from
                {{ source("PROD_LANDING", source_table) }},
                lateral flatten(input => parse_json(source_json)) as flat_json
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
                '{{ chain }}' as chain,
                '{{ app }}' as app,
                'DeFi' as category,
                sum(borrows_usd) as daily_borrows_usd,
                sum(supply_usd) as daily_supply_usd
            from map_reduce_json
            group by date
        )
    select date, chain, app, category, daily_borrows_usd, daily_supply_usd
    from combined_data
{% endmacro %}
