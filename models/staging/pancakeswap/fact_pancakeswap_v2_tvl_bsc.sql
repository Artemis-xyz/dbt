-- {{ config(materialized="incremental", unique_key="date") }}
{{ config(materialized="table", snowflake_warehouse="PANCAKESWAP_TVL_SM") }}


{{
    fact_daily_uniswap_v2_fork_tvl(
        "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73",
        "bsc",
        "pancakeswap_v2",
    )
}}

-- (
-- "0x02fb18bfee3ce825fcec2456b1c603ed6273b6ef",
-- "0x71161dbe3b387776c37c49f44926ec5fe47de32c",
-- "0x5b3345be7c6febc2ec4a6d459bb1531411486ba7",
-- "0xd9e26d20b81f03f1241f31127d610bd120a42bf4",
-- "0x0eda99edc38abe8321d504a194827a6d2fc487d2",
-- "0x83cab2b23260cf972c5a309795af05b4e6e8a8b9",
-- "0xac441ba3ac12fcbfbdedb5e174ab597ca016f484",
-- "0xf17089a946527dd57ab8b172ddd7fd27d44f8ba6",
-- "0x606d607401f27e687770c404dff25131150f0a04",
-- "0x882aa286ac8c0ed68e21d817a01f452715f32bbd",
-- ),

