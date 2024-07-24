{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_dssvesttransferrable_yank"
    )
}}


select
    *
from
    ethereum_flipside.core.fact_event_logs
where
    lower(contract_address) = lower('0x6D635c8d08a1eA2F1687a5E46b666949c977B7dd')
    and lower(topics [0]) = '0x6f2a3ed78a3066d89360b6c89e52bf3313f52e859401a3ea5fa0f033fd540c3c'