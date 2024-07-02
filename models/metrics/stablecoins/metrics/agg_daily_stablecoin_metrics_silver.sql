{{ config(materialized="table") }}
with
    daily_data as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("agg_arbitrum_stablecoin_metrics"),
                    ref("agg_avalanche_stablecoin_metrics"),
                    ref("agg_base_stablecoin_metrics"),
                    ref("agg_blast_stablecoin_metrics"),
                    ref("agg_bsc_stablecoin_metrics"),
                    ref("agg_celo_stablecoin_metrics"),
                    ref("agg_ethereum_stablecoin_metrics"),
                    ref("agg_optimism_stablecoin_metrics"),
                    ref("agg_polygon_stablecoin_metrics"),
                    ref("agg_solana_stablecoin_metrics"),
                    ref("agg_ton_stablecoin_metrics"),
                    ref("agg_tron_stablecoin_metrics"),
                ]
            )
        }}
    )
select *, date::string || '-' || chain || '-' || symbol || contract_address as unique_id
from daily_data
where date < to_date(sysdate())
