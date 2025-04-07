{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_dssvesttransferrable_create"
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
and left(input, 10) = '0xdb64ff8f'
)
SELECT
    block_timestamp,
    tx_hash,
    '0x' || SUBSTR(raw_input_data, 25, 40) as _usr,
    PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(raw_input_data, 65, 64)) as _tot,
    PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(raw_input_data, 129, 64)) as _bgn,
    PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(raw_input_data, 193, 64)) as _tau,
    PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(raw_input_data, 257, 64)) as _eta,
    ROW_NUMBER() OVER (ORDER BY block_timestamp, trace_index) AS output_id
FROM raw