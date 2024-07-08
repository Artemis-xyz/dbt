{{ config(materialized="view", snowflake_warehouse="FANTOM") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_fantom_gas") }}
    ),
    data as (
        select to_date(value:"date"::string) as date, value:"gas"::float as gas
        from
            {{ source("PROD_LANDING", "raw_fantom_gas") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    ),
    ftm_price as ({{ get_coingecko_price_with_latest("fantom") }})
select
    data.date,
    'fantom' as chain,
    gas,
    gas * coalesce(price, 0) as gas_usd,
    gas_usd as fees,
    fees * .15 as revenue
from data
left join ftm_price on data.date = ftm_price.date
where data.date < to_date(sysdate())
order by date desc
