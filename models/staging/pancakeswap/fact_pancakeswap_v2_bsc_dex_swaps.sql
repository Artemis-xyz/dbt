{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}

{{
    fact_uniswap_v2_fork_dex_swaps(
        "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73",
        "bsc",
        (
            "0x02fb18bfee3ce825fcec2456b1c603ed6273b6ef",
            "0x71161dbe3b387776c37c49f44926ec5fe47de32c",
            "0x5b3345be7c6febc2ec4a6d459bb1531411486ba7",
            "0xd9e26d20b81f03f1241f31127d610bd120a42bf4",
            "0x0eda99edc38abe8321d504a194827a6d2fc487d2",
            "0x7a3dc277b07947754b23dc4e579c59830baa8490",
            "0x53009cb4004cc3041b49888b56a9c7a73db8b4ab"
        ),
        "pancakeswap",
        2500,
        version="v2"
    )
}}
