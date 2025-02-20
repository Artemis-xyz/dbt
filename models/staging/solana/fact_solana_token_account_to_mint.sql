{{
    config(
        materialized="incremental",
        snowflake_warehouse="SOLANA",
        unique_key=["account_address", "owner", "mint"],
    )
}}

-- This table maps all token accounts to their corresponding token mint and owner accounts.
-- This is data not provided in Flipside's fact_token_account_owners table.

select
    distinct
    account_address,
    owner,
    mint
from
    solana_flipside.core.fact_token_balances
where 1=1
    and owner is not null -- some duplicates in source table where owner is null
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from solana_flipside.core.fact_token_balances) -- 3 day lookback on source table
    {% endif %}
