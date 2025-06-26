{{
    config(
        materialized='table',
        snowflake_warehouse='STABLECOIN_MONTHLY'
    )
}}

{{ dbt_utils.union_relations(
    relations=[
        ref("agg_weekly_stablecoin_breakdown_category_chain"),
        ref("agg_weekly_stablecoin_breakdown_category_symbol_chain"),
        ref("agg_weekly_stablecoin_breakdown_category_symbol"),
        ref("agg_weekly_stablecoin_breakdown_category"),
        ref("agg_weekly_stablecoin_breakdown_chain"),
        ref("agg_weekly_stablecoin_breakdown_symbol_chain"),
        ref("agg_weekly_stablecoin_breakdown_symbol"),
    ],
    include=["date_granularity", "symbol", "category", "chain", "stablecoin_dau", "stablecoin_transfer_volume", "stablecoin_daily_txns", "stablecoin_avg_txn_value", "artemis_stablecoin_dau", "artemis_stablecoin_transfer_volume", "artemis_stablecoin_daily_txns", "artemis_stablecoin_avg_txn_value", "p2p_stablecoin_dau", "p2p_stablecoin_transfer_volume", "p2p_stablecoin_daily_txns", "p2p_stablecoin_avg_txn_value", "stablecoin_supply", "p2p_stablecoin_supply"]
) }}