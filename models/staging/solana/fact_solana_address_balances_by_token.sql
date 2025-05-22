{{
    config(
        materialized="incremental",
        unique_key=["address", "tx_id", "contract_address", "index"],
        snowflake_warehouse="SOLANA_XLG",
    )
}}
-- Solana Token Balances
select
    tx_id,
    account_address as address,
    mint as contract_address,
    balance as amount,
    null as decimals,
    null as amount_unadj,
    fact_token_balances_id as index,
    block_timestamp
from solana_flipside.core.fact_token_balances
where
    succeeded = 'TRUE'
    and block_timestamp::date < to_date(sysdate())
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}

-- Solana Native Balances
union all
select
    tx_id,
    value:"pubkey"::string as address,
    'native_token' as contract_address,
    post_balances[index]::float / pow(10, 9) as amount,
    9 as decimals,
    post_balances[index]::float as amount_unadj,
    index::string as index,
    block_timestamp
from solana_flipside.core.fact_transactions, lateral flatten(input => account_keys)
where
    succeeded = 'TRUE'
    and to_date(block_timestamp) < to_date(sysdate())
    and post_balances[index] <> pre_balances[index]
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}

union all
-- Solana Token Balances (Staking)
select
    sha2(
        hash(stake_pubkey, block_timestamp, 'native_token', post_balance_sol), 512
    ) as tx_id,
    stake_pubkey as address,
    'native_token' as contract_address,
    post_balance_sol as amount,
    9 as decimals,
    post_balance_sol * pow(10, 9) as amount_unadj,
    '0' as index,
    block_timestamp
from solana_flipside.gov.fact_rewards_staking
where
    to_date(block_timestamp) < to_date(sysdate())
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
