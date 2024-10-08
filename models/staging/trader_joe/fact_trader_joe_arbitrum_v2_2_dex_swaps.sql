{{
    config(
        materialized="table",
        snowflake_warehouse="TRADER_JOE",
    )
}}

{{get_trader_joe_v_2_2_swaps_for_chain('0xb43120c4745967fa9b93E79C149E66B0f2D6Fe0c', 'arbitrum', 'v2_2')}}