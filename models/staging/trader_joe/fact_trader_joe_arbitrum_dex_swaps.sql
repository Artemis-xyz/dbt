{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
    )
}}

{{
    fact_uniswap_v2_fork_dex_swaps(
        "0xaE4EC9901c3076D0DdBe76A520F9E90a6227aCB7",
        "arbitrum",
        (),
        "trader_joe",
        3000,
        version="v1"
    )
}}
