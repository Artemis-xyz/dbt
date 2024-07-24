{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_dssvesttransferrable_create"
    )
}}


select
    *
from
    ethereum_flipside.core.fact_event_logs
where
    lower(contract_address) = lower('0x6D635c8d08a1eA2F1687a5E46b666949c977B7dd')
    and lower(topics [0]) = '0x2e3cc5298d3204a0f0fc2be0f6fdefcef002025f4c75caf950b23e6cfbfb78d0'