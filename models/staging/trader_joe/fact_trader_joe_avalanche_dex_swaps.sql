{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="TRADER_JOE",
    )
}}

{{
    fact_uniswap_v2_fork_dex_swaps(
        "0x9Ad6C38BE94206cA50bb0d90783181662f0Cfa10",
        "avalanche",
        (),
        "trader_joe",
        3000,
        version="v1"
    )
}}
