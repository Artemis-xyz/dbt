{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="SEI_LG",
    )
}}
WITH transaction_data as (
    SELECT
        tx_hash,
        status = 'SUCCESS' as success,
        block_timestamp,
        raw_date,
        from_address,
        tx_fee,
        gas_usd,
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
        stablecoin_balance,
        last_updated_timestamp
    FROM {{ ref("fact_sei_evm_transactions_v2") }}
    {% if is_incremental() %}
    where
        -- this filter will only be applied on an incremental run 
        inserted_timestamp
        >= (select dateadd('day', -3, max(inserted_timestamp)) from {{ this }})
        or 
        last_updated_timestamp
        >= (select dateadd('day', -3, max(last_updated_timestamp)) from {{ this }})
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
        stablecoin_balance,
        last_updated_timestamp
    FROM {{ ref("fact_sei_wasm_transactions_v2") }}
    {% if is_incremental() %}
    where
        -- this filter will only be applied on an incremental run 
        inserted_timestamp
        >= (select dateadd('day', -3, max(inserted_timestamp)) from {{ this }})
        or 
        last_updated_timestamp
        >= (select dateadd('day', -3, max(last_updated_timestamp)) from {{ this }})
    {% endif %}
)
SELECT
    tx_hash,
    max(success) as success,
    max(block_timestamp) as block_timestamp,
    max(raw_date) as raw_date,
    max(from_address) as from_address,
    max(tx_fee) as tx_fee,
    max(gas_usd) as gas_usd,
    'sei' as chain,
    max(contract_address) as contract_address,
    max(name) as name,
    max(app) as app,
    max(friendly_name) as friendly_name,
    max(sub_category) as sub_category,
    max(inserted_timestamp) as inserted_timestamp,
    max(category) as category,
    max(user_type) as user_type,
    max(address_life_span) as address_life_span,
    max(cur_total_txns) as cur_total_txns,
    max(cur_distinct_to_address_count) as cur_distinct_to_address_count,
    max(probability) as probability,
    max(engagement_type) as engagement_type,
    max(balance_usd) as balance_usd,
    max(native_token_balance) as native_token_balance,
    max(stablecoin_balance) as stablecoin_balance,
    max(last_updated_timestamp) as last_updated_timestamp
FROM transaction_data
WHERE block_timestamp <= date_trunc('day', sysdate())
GROUP BY tx_hash
