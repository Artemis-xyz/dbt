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
    burn_data.total_burns,
    burn_data.revenue,
    price_data.price
from burn_data
left join price_data using(date)