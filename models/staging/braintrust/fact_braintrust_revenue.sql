{{
    config(
        materialized="table",
        snowflake_warehouse="BRAINTRUST",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_braintrust_revenue") }}
        
    ),
   latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_braintrust_revenue") }}
        where extraction_date = (select max_date from max_extraction)
    )
    , flattened_data as (
        select
            f.value:block_date::date as date,
            f.value:tokens_burned::number as burns_native
        from latest_data, lateral flatten(input => data) as f
    )
select 
    date, 
    burns_native,
    burns_native * p.price as burns
from flattened_data
left join {{ source("ETHEREUM_FLIPSIDE_PRICE", "ez_prices_hourly")}} p on (
    p.hour = date 
    and lower(p.token_address) = lower('0x799ebfabe77a6e34311eeee9825190b9ece32824')
)
where date < to_date(sysdate())
order by date desc