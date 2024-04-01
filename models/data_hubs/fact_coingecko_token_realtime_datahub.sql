{{ config(materialized="table", snowflake_warehouse="PROD_REALTIME_DATAHUB") }}
select *
from {{ ref("fact_coingecko_token_realtime_data") }}
