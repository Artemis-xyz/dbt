{{ config(materialized="table", snowflake_warehouse="BITCOIN") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_bitcoin_hodl_wave") }}
    )
select
    date(to_timestamp(value:date::number / 1000)) as date,
    value:total_utxo_value::float as total_utxo_value,
    value:utxo_value_under_1d::float as utxo_value_under_1d,
    value:utxo_value_1d_1w::float as utxo_value_1d_1w,
    value:utxo_value_1w_1m::float as utxo_value_1w_1m,
    value:utxo_value_1m_3m::float as utxo_value_1m_3m,
    value:utxo_value_3m_6m::float as utxo_value_3m_6m,
    value:utxo_value_6m_12m::float as utxo_value_6m_12m,
    value:utxo_value_1y_2y::float as utxo_value_1y_2y,
    value:utxo_value_2y_3y::float as utxo_value_2y_3y,
    value:utxo_value_3y_5y::float as utxo_value_3y_5y,
    value:utxo_value_5y_7y::float as utxo_value_5y_7y,
    value:utxo_value_7y_10y::float as utxo_value_7y_10y,
    value:utxo_value_greater_10y::float as utxo_value_greater_10y,
    value as source,
    'bitcoin' as chain
from
    {{ source("PROD_LANDING", "raw_bitcoin_hodl_wave") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
