{{ config(materialized="table", snowflake_warehouse="STABLECOIN_LG_2") }}
with
    all_data as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("agg_arbitrum_stablecoin_breakdown_monthly"),
                    ref("agg_avalanche_stablecoin_breakdown_monthly"),
                    ref("agg_base_stablecoin_breakdown_monthly"),
                    ref("agg_bsc_stablecoin_breakdown_monthly"),
                    ref("agg_celo_stablecoin_breakdown_monthly"),
                    ref("agg_ethereum_stablecoin_breakdown_monthly"),
                    ref("agg_optimism_stablecoin_breakdown_monthly"),
                    ref("agg_polygon_stablecoin_breakdown_monthly"),
                    ref("agg_solana_stablecoin_breakdown_monthly"),
                    ref("agg_ton_stablecoin_breakdown_monthly"),
                    ref("agg_tron_stablecoin_breakdown_monthly"),
                ]
            )
        }}
    )
select *, chain || '-' || from_address || '-' || contract_address as unique_id
from all_data
