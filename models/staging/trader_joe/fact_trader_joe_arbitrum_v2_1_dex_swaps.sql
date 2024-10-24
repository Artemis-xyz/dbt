{{
    config(
        materialized="table",
        snowflake_warehouse="TRADER_JOE",
    )
}}

{{get_trader_joe_v_2_1_swaps_for_chain('0x8e42f2F4101563bF679975178e880FD87d3eFd4e', 'arbitrum', 'v2.1')}}