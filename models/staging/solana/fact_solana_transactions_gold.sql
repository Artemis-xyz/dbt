{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BAM_TRANSACTION_LG",
    )
}}

select
    tx_hash,
    raw_date,
    block_timestamp,
    signers,
    program_id,
    chain,
    tx_fee,
    gas_usd,
    succeeded,
    token_address,
    token_name,
    name,
    app,
    friendly_name,
    sub_category,
    category,
    user_type,
    address_life_span,
    cur_total_txns,
    cur_distinct_to_address_count,
    balance_usd,
    native_token_balance,
    stablecoin_balance
from {{ ref("fact_solana_transactions") }}
where
    raw_date < to_date(sysdate())
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -5, max(block_timestamp)) from {{ this }})
{% else %}
    -- Making code not compile on purpose. Full refresh of entire history takes too
    -- long, doing last month will wipe out backfill
    -- TODO: Figure out a workaround.
    where
        block_timestamp
        >= (select dateadd('month', -1, max(block_timestamp)) from {{ this }})
{% endif %}
