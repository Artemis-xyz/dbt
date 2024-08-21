{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="SEI_LG",
    )
}}
SELECT
    tx_hash,
    status = 'SUCCESS' as success,
    block_timestamp,
    raw_date,
    from_address,
    tx_fee,
    gas_usd,
    'sei' as chain,
    contract_address,
    name,
    app,
    friendly_name,
    sub_category,
    inserted_timestamp,
    category,
    user_type,
    address_life_span,
    cur_total_txns,
    cur_distinct_to_address_count,
    probability,
    engagement_type,
    balance_usd,
    native_token_balance,
    stablecoin_balance
FROM {{ ref("fact_sei_evm_transactions") }}
{% if is_incremental() %}
where
    -- this filter will only be applied on an incremental run 
    inserted_timestamp
    >= (select dateadd('day', -5, max(inserted_timestamp)) from {{ this }})
{% endif %}
UNION ALL
SELECT
    tx_hash,
    success,
    block_timestamp,
    raw_date,
    signer as from_address,
    tx_fee,
    gas_usd,
    'sei' as chain,
    contract_address,
    name,
    app,
    friendly_name,
    sub_category,
    inserted_timestamp,
    category,
    user_type,
    address_life_span,
    cur_total_txns,
    cur_distinct_to_address_count,
    probability,
    engagement_type,
    balance_usd,
    native_token_balance,
    stablecoin_balance
FROM {{ ref("fact_sei_wasm_transactions") }}
{% if is_incremental() %}
where
    -- this filter will only be applied on an incremental run 
    inserted_timestamp
    >= (select dateadd('day', -5, max(inserted_timestamp)) from {{ this }})
{% endif %}