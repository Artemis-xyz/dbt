{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="SUI_MD",
        database="sui",
        schema="raw",
        alias="ez_transactions",
    )
}}

select
    tx_hash,
    raw_date,
    block_timestamp,
    sender,
    tx_fee,
    gas_usd,
    native_revenue,
    revenue,
    package,
    name,
    app,
    friendly_name,
    sub_category,
    category,
    status,
    chain,
    null as user_type,
    null as balance_usd,
    null as native_token_balance,
    null as stablecoin_balance,
    null as probability,
    null as engagement_type
from {{ ref("fact_sui_transactions") }}
where
    raw_date < to_date(sysdate())
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -5, max(block_timestamp)) from {{ this }})
    {% endif %}
