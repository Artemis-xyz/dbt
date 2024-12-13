{{
    config(
        materialized="incremental",
        unique_key=["tx_id", "log_messages", "action"],
        snowflake_warehouse="HIVEMAPPER",
    )
}}

-- mints
with events as (
        {{ get_solana_token_mints_burns_transfers('4vMsoUT2BWatFweudnQM1xedRLfJgJ7hswhcpz4xgBTy') }}
)

SELECT
    tx.block_timestamp,
    tx.tx_id,
    tx.log_messages,
    e.action,
    e.tx_to_account,
    e.tx_from_account,
    e.amount_native / 1e9 as amount_native
from
    events e
    join solana_flipside.core.fact_transactions tx on e.tx_id = tx.tx_id
where
    1 = 1
    {% if is_incremental() %}
        and tx.block_timestamp >= (SELECT dateadd('day', -3, max(block_timestamp)) FROM {{ this }})
    {% else %}
        and tx.block_timestamp >= '2022-11-01'
    {% endif %}
    and succeeded