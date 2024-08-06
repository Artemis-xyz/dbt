{{ config(materialized="table", snowflake_warehouse="X_SMALL") }}
select *
from {{ ref("fact_coingecko_token_realtime_data") }}
