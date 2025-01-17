{{
    config(
        materialized="table",
        snowflake_warehouse="SOLANA",
        database="solana",
        schema="raw",
        alias="ez_token_account_mappings",
    )
}}

select 
    account_address
    , owner
    , start_b.block_timestamp as start_timestamp
    , coalesce(end_b.block_timestamp, sysdate()) as end_timestamp
from solana_flipside.core.fact_token_account_owners 
left join solana_flipside.core.fact_blocks start_b on start_block_id = start_b.block_id
left join solana_flipside.core.fact_blocks end_b on end_block_id = end_b.block_id