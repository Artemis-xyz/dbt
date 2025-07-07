{{config(materialized="table", snowflake_warehouse='STELLAR')}}

select 
    parquet_raw:date::date as date,
    MAX(parquet_raw:original_supply::NUMBER) as original_supply,
    MAX(parquet_raw:inflation_max_supply::NUMBER) as inflation_max_supply,
    MAX(parquet_raw:max_supply::NUMBER) as max_supply,
    MAX(parquet_raw:uncreated_tokens::NUMBER) as uncreated_tokens,
    MAX(parquet_raw:total_supply::NUMBER) as total_supply,
    MAX(parquet_raw:burned_tokens::NUMBER) as burned_tokens,
    MAX(parquet_raw:foundation_balances::NUMBER) as foundation_balances,
    MAX(parquet_raw:issued_supply::NUMBER) as issued_supply,
    MAX(parquet_raw:unvested_tokens::NUMBER) unvested_tokens,
    MAX(parquet_raw:circulating_supply_native::NUMBER) as circulating_supply_native
from {{ source("PROD_LANDING", "raw_stellar_fact_stellar_issued_supply_and_float_parquet") }}
group by date
