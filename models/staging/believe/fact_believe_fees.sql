{{ 
    config(
        materialized = 'incremental',
        unique_key = 'date',
        incremental_strategy = 'merge',
        snowflake_warehouse = 'BELIEVE',
    )
}}

with
    believe_bonding_curve_referral_fees as (
        select
            block_timestamp
            , tx_id
            , instruction:parsed:info:tokenAmount:amount::STRING as raw_amount
            , instruction:parsed:info:tokenAmount:uiAmount::STRING as amount_native
        from solana_flipside.core.fact_events_inner
        where
            instruction:parsed:info:destination = '9koN38T5C8k4GBLmZkU95XGNH2UEcnvKjK9v7XpkgQAR' -- Referral Address
            and event_type = 'transferChecked' -- Event Type for Transfer Checked
            and succeeded = true -- Success Indicator
            {% if is_incremental() %}
                and date(block_timestamp) >= (select dateadd('day', -3, max(date)) from {{ this }})
            {% endif %}
    )

    , price_data as (
        {{ get_coingecko_metrics("solana") }}
    )

    , referral_fees_usd as (
        select
            date(bbcrf.block_timestamp) as date
            , sum(coalesce(raw_amount, 0)) as raw_amount
            , sum(coalesce(amount_native, 0)) as amount_native
            , sum(coalesce(amount_native, 0)) * coalesce(p.price, 0) as amount_usd
        from believe_bonding_curve_referral_fees bbcrf
        left join price_data p 
            on date(bbcrf.block_timestamp) = p.date
        group by date(bbcrf.block_timestamp), p.price
    )

    , graduating_tokens_address as (
        select
            block_timestamp
            , tx_id
            , decoded_accounts[20]:pubkey::string as protocol_fee_token
            , decoded_accounts[7]:pubkey::string as token_address
            , signers[0]::string as signer
            , event_type
            , program_id
        from solana_flipside.core.ez_events_decoded
        where 1=1
            and event_type in ('migrate_meteora_damm', 'migration_damm_v2') -- Event Types for Graduating Tokens into Meteora Pools
            and program_id = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN' -- Meteora Dynamic Bonding Curve Program ID
        )

    , graduating_tokens_fees as (
        select 
            fei.block_timestamp
            , fei.tx_id
            , fei.signers[0]::string as signers
            , fei.instruction:parsed:info:amount::number / 1e9 as amount_native
            , fei.instruction:parsed:info:destination::string as protocol_fee_token
            , fei.instruction:parsed:info:source::string as from_address
            , instruction_program_id
            , swap_to_amount_usd / swap_from_amount as token_conversion_price
            , amount_native * token_conversion_price as amount_usd
        from solana_flipside.core.fact_events_inner fei
        left join solana_flipside.defi.ez_dex_swaps eds on fei.tx_id = eds.tx_id and instruction_program_id = eds.program_id
        where 1=1
            and instruction_index = 4 and inner_index = 0 -- Instructions Index for Graduating Tokens Fees
            and eds.program_id = 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB' -- Meteora Pools Program ID
            and fei.program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' -- Token Program ID
            and fei.instruction:parsed:info:destination::string in (select protocol_fee_token from graduating_tokens_address)
            and succeeded = true
            {% if is_incremental() %}
                and date(fei.block_timestamp) >= (select dateadd('day', -3, max(date)) from {{ this }})
            {% endif %}
    )
    , agg_graduating_fees_usd as (
        select
            date(gtf.block_timestamp) as date
            , sum(amount_native) as amount_native
            , sum(amount_usd) as amount_usd
        from graduating_tokens_fees gtf
        group by 1
    )

    , total_fees as (
        select
            date
            , coalesce(agfu.amount_native, 0) + coalesce(rfu.amount_native, 0) as amount_native
            , coalesce(agfu.amount_usd, 0) + coalesce(rfu.amount_usd, 0) as amount_usd
        from agg_graduating_fees_usd agfu
        left join referral_fees_usd rfu using (date)
    )

select
    date
    , amount_native
    , amount_usd
from
    total_fees
