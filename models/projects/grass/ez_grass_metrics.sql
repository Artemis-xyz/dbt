{{
    config(
        materialized="table",
        snowflake_warehouse="GRASS",
        database="grass",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    data_collected as (
        select
            date,
            data_collected_tb
        from {{ ref("fact_grass_data_scraped") }}
    )
, market_data as (
    {{ get_coingecko_metrics('grass')}}
)

select
    market_data.date,
    data_collected.data_collected_tb,
    market_data.price,
    market_data.market_cap,
    market_data.fdmc,
    market_data.token_turnover_circulating,
    market_data.token_turnover_fdv,
    market_data.token_volume
from market_data
left join data_collected on market_data.date = data_collected.date