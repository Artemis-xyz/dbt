{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_dssvesttransferrable_vest"
    )
}}


select
    *
from
    ethereum_flipside.core.fact_event_logs
where
    lower(contract_address) = lower('0x6D635c8d08a1eA2F1687a5E46b666949c977B7dd')
    and lower(topics [0]) = '0xa2906882572b0e9dfe893158bb064bc308eb1bd87d1da481850f9d17fc293847'