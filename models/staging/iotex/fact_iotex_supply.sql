{{
    config(
        materialized="table",
        snowflake_warehouse="IOTEX"
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_iotex_supply_metrics") }}
    )
    , price as (
        {{ get_coingecko_price_with_latest("iotex")}}
    )
    , data as (
        select
            value:date::date as date,
            value:burn::number as burn,
            value:issue::number as mints,
            value:circulating_supply::number as circulating_supply
        from
            {{ source("PROD_LANDING", "raw_iotex_supply_metrics") }},
            lateral flatten(input => parse_json(source_json))
    )
select
    data.date,
    max(data.burn) as burn,
    max(data.mints) as mints,
    max(data.circulating_supply) as circulating_supply,
    max(data.burn * price) as burn_usd,
    max(data.mints * price) as mints_usd
from data
left join price using (date)
where data.date < to_date(sysdate())
group by data.date
order by data.date desc