{{
    config(
        materialized="incremental",
        unique_key=["block_timestamp", "block_number", "transaction_index", "trust_line_index", "contract_address", "address", "balance_raw"],
        snowflake_warehouse="RIPPLE",
    )
}}

select
    datetime as block_timestamp,
    ledger_index as block_number,
    transaction_index,
    trust_line_index,
    token_address as contract_address,
    account_address as address,
    balance as balance_raw
from {{ source("SONARX_XRP_BALANCES", "token_balances") }}
{% if is_incremental() %}
    where block_timestamp >= (select dateadd(day, -3, max(block_timestamp)) from {{ this }})
{% endif %}