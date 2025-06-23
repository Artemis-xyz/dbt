{{ 
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "event_index"],
        snowflake_warehouse="RIPPLE"
    )
}}

select
    datetime as block_timestamp,
    ledger_index as block_number,
    transaction_hash,
    transaction_index,
    transfer_index as event_index,
    token_address as contract_address,
    from_address,
    to_address,
    source_value as amount_raw,
    value as amount_native,
    usd_value as amount_usd
from {{ source("SONARX_XRP", "priced_transfers") }}
{% if is_incremental() %}
    where block_timestamp >= (select dateadd(day, -3, max(block_timestamp)) from {{ this }})
{% endif %}
