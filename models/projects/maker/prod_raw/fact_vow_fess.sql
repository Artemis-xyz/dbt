{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_vow_fess"
    )
}}

SELECT
    block_timestamp,
    pc_dbt_db.prod.hex_to_int(topics[2])::double/1e45 as tab,
    tx_hash
FROM ethereum_flipside.core.fact_event_logs
where topics[0] = '0x697efb7800000000000000000000000000000000000000000000000000000000'
and contract_address = lower('0xA950524441892A31ebddF91d3cEEFa04Bf454466')