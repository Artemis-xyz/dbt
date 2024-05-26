-- depends_on: {{ source("PROD_LANDING", "raw_swell_restaked_eth_count") }}
{{ config(materialized="table") }}
with
    dates as (
        select
            extraction_date,
            flat_json.value:"date"::timestamp as date
        from
            {{ source("PROD_LANDING", "raw_swell_restaked_eth_count")}},
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
            flat_json.value:"date"::timestamp as date,
            flat_json.value:"total_restaked_eth" as total_supply
        from
            {{ source("PROD_LANDING", "raw_swell_restaked_eth_count")}},
            lateral flatten(input => parse_json(source_json)) as flat_json
    ),
    map_reduce_json as (
        select t1.*
        from flattened_json t1
        left join max_extraction_per_day t2 on t1.date = t2.date
        where t1.extraction_date = t2.extraction_date
    )
select *, 'ethereum' as chain
from map_reduce_json
where date < to_date(sysdate())