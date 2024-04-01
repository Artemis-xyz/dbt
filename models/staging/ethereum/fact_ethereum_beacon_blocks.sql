{{ config(materialized="incremental", unique_key="block_number") }}
select
    activation_eligibility_epoch,
    activation_epoch,
    balance,
    block_number,
    effective_balance,
    exit_epoch,
    fact_validators_id,
    index,
    pubkey,
    slashed,
    slot_number,
    state_id,
    validator_status,
    withdrawable_epoch,
    withdrawal_credentials,
    sysdate() as inserted_timestamp
from ethereum_flipside.beacon_chain.fact_validators
