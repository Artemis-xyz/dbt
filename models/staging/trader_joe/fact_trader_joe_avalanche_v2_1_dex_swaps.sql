{{
    config(
        materialized="incremental",
        snowflake_warehouse="TRADER_JOE",
    )
}}

{{get_trader_joe_v_2_1_swaps_for_chain('0x8e42f2F4101563bF679975178e880FD87d3eFd4e', 'avalanche', 'v2.1')}}