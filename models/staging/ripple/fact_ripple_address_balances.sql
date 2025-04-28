
{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address", "block_timestamp"],
        snowflake_warehouse="RIPPLE",
    )
}}

select
    datetime as block_timestamp,
    ledger_index as block_number,
    token_address as contract_address,
    account_address as address,
    balance as balance_raw
from {{ source("SONARX_XRP_BALANCES", "token_balances") }}
{% if is_incremental() %}
    where block_timestamp >= (select dateadd(day, -3, max(block_timestamp)) from {{ this }})
{% endif %}