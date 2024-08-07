{% macro raw_aave_rpc_data(raw_source_table, healed_source_table) %}
with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        {% if healed_raw_table is defined %}
            select *
            from landing_database.prod_landing.{{healed_raw_table}}
            union all
        {% endif %}
        select *
        from {{ source("PROD_LANDING", raw_source_table) }}
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
            flat_json.value:"underlying_token"::string as underlying_token,
            flat_json.value:"underlying_token_price"::float as underlying_token_price,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    )
select t1.*
from flattened_json t1
left join max_extraction_per_day t2 on t1.date = t2.date
where t1.extraction_date = t2.extraction_date
{% endmacro %}