{{
    config(
        materialized="table",
        snowflake_warehouse="RENDER",
        database="render",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    burn_data as (
        select 
            block_timestamp::date as date,
            sum(amount_native) as total_burns,
            sum(amount) as revenue
        from {{ ref("fact_render_burns") }}
        group by 1
    )
    , price_data as (
        {{ get_coingecko_metrics("render") }}
    )

select
    burn_data.date,
    price_data.price,
    burn_data.total_burns as burns_native,
    burn_data.revenue
from burn_data
left join price_data using(date)