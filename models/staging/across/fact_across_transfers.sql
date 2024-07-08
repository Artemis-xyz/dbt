{{
    config(
        materialized="table",
        snowflake_warehouse="BRIDGE_MD",
    )
}}

select
    'v1' as version,
    contract_address,
    block_timestamp,
    tx_hash,
    event_index,
    amount,
    depositor,
    recipient,
    destination_chain_id,
    destination_token,
    origin_chain_id,
    realized_lp_fee_pct,
    relayer_fee_pct,
    null as destination_token_symbol,
    null as input_amount,
    null as input_token
from {{ ref("fact_across_v1_transfers") }}
where origin_chain_id != 1919191  -- a bug
union all
select
    'v2' as version,
    contract_address,
    block_timestamp,
    tx_hash,
    event_index,
    amount,
    depositor,
    recipient,
    destination_chain_id,
    destination_token,
    origin_chain_id,
    realized_lp_fee_pct,
    relayer_fee_pct,
    destination_token_symbol,
    null as input_amount,
    null as input_token
from {{ ref("fact_across_v2_transfers") }}
union all
select
    'uba' as version,
    contract_address,
    block_timestamp,
    tx_hash,
    event_index,
    amount,
    depositor,
    recipient,
    destination_chain_id,
    destination_token,
    origin_chain_id,
    realized_lp_fee_pct,
    relayer_fee_pct,
    null as destination_token_symbol,
    null as input_amount,
    null as input_token
from {{ ref("fact_across_uba_transfers") }}
union all
select
    'v3' as version,
    contract_address,
    block_timestamp,
    tx_hash,
    event_index,
    amount,
    depositor,
    recipient,
    destination_chain_id,
    destination_token,
    origin_chain_id,
    null as realized_lp_fee_pct,
    null as relayer_fee_pct,
    destination_token_symbol,
    input_amount,
    input_token
from {{ ref("fact_across_v3_transfers") }}
where destination_chain_id != 0  -- a bug
-- Artemis currently does not support lisk chain and OP l2
    and destination_chain_id != 1125 
    and origin_chain_id != 1125
