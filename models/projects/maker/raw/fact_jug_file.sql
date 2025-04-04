{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_jug_file"
    )
}}


SELECT 
    block_timestamp,
    pc_dbt_db.prod.HEX_TO_UTF8(rtrim(topics[2],0)) as ilk,
    tx_hash
FROM ethereum_flipside.core.fact_event_logs
where topics[0] = '0x29ae811400000000000000000000000000000000000000000000000000000000'
and contract_address ilike '0x19c0976f590D67707E62397C87829d896Dc0f1F1'