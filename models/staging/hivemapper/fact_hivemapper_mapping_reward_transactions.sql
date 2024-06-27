{{
    config(
        materialized="incremental",
        snowflake_warehouse="HIVEMAPPER_LG"
    )
}}

{% set backfill_date = null %}

select *
from solana_flipside.core.fact_transactions,
    lateral flatten(input => account_keys)
where value:"pubkey"::string = 'BNH1dUp3ExFbgo3YctSqQbJXRFn3ffkwbcmSas8azfaW'
    and array_contains('Program log: Instruction: MintTo'::variant, log_messages) --mint Instruction
    and (
        array_contains('Program log: Instruction: PayTo'::variant, log_messages) -- only mapping rewards contains PayTo log
        or -- dec 11 -- -8.7M
        array_contains('Program log: Memo (len 12): "Map Coverage"'::variant, log_messages)
    )
    and block_timestamp > '2022-11-01' --start of protocol
    and array_size(pre_token_balances) > 0
    and array_size(post_token_balances) > 0
    and succeeded = 'TRUE'
    {% if backfill_date != null %}
        and block_timestamp <= '{{ backfill_date }}'
    {% endif %}
    {% if is_incremental() %}
        and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}