{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
        unique_key = ["tx_hash", "event_index"]
    )
}}

{{
    dbt_utils.union_relations(
        relations=[
            ref('fact_pendle_arbitrum_amm_swaps'),
            ref('fact_pendle_base_amm_swaps'),
            ref('fact_pendle_bsc_amm_swaps'),
            ref('fact_pendle_ethereum_amm_swaps'),
            ref('fact_pendle_optimism_amm_swaps'),
        ]
    )
}}