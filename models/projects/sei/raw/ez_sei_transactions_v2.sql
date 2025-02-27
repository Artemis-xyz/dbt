{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="SEI_LG",
        database="SEI",
        schema="raw",
        alias="ez_transactions_v2",
    )
}}
SELECT
    tx_hash,
    success,
    block_timestamp,
    raw_date,
    from_address,
    tx_fee,
    gas_usd,
    chain,
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
FROM {{ ref("fact_sei_transactions_v2") }}
{% if is_incremental() %}
where
    -- this filter will only be applied on an incremental run 
    inserted_timestamp
    >= (select dateadd('day', -5, max(inserted_timestamp)) from {{ this }})
{% endif %}
