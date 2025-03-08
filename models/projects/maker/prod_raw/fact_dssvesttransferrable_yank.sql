{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_dssvesttransferrable_yank"
    )
}}

with raw as (
SELECT
    trace_index,
    trace_address,
    block_timestamp,
    tx_hash,
    SUBSTR(input, 11) as raw_input_data
FROM ethereum_flipside.core.fact_traces
where to_address = lower('0x6D635c8d08a1eA2F1687a5E46b666949c977B7dd')
and left(input, 10) in ('0x509aaa1d', '0x26e027f1')
)
SELECT 
    block_timestamp,
    tx_hash,
    PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(raw_input_data, 1, 64)) as _id,
    PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(raw_input_data, 65, 64)) as _end
FROM raw