{{
    config(
        materialized="view",
        snowflake_warehouse="TON",
        database="ton",
        schema="raw",
        alias="ez_p2p_stablecoin_transfers",
    )
}}

select * 
from {{ref("fact_ton_p2p_stablecoin_transfers")}}