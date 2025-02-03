{{
    config(
        materialized="table",
        snowflake_warehouse="CELESTIA"
    )
}}
with tia_prices as (
    {{ get_coingecko_price_with_latest('celestia')}}
)
select
    date
    , mints
    , mints * price as mints_usd
from {{ ref("fact_celestia_mints_silver") }}
left join tia_prices using (date)