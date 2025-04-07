{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_spot_file"
    )
}}


SELECT 
    block_timestamp,
    pc_dbt_db.prod.HEX_TO_UTF8(rtrim(topics[2], 0)) as ilk,
    tx_hash
FROM ethereum_flipside.core.fact_event_logs
where topics[0] = '0x1a0b287e00000000000000000000000000000000000000000000000000000000'
and contract_address ilike '0x65C79fcB50Ca1594B025960e539eD7A9a6D434A3'