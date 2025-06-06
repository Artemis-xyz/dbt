{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
    )
}}

{{get_pendle_reward_fees_for_chain_by_token('ethereum')}}