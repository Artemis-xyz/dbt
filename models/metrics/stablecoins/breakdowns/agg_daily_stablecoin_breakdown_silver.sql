{{ config(materialized="table", snowflake_warehouse="STABLECOIN_V2_LG") }}
with
    daily_data as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("agg_base_daily_stablecoin_metrics_breakdown"),
                    ref("agg_blast_daily_stablecoin_metrics_breakdown"),
                    ref("agg_arbitrum_daily_stablecoin_metrics_breakdown"),
                    ref("agg_optimism_daily_stablecoin_metrics_breakdown"),
                    ref("agg_avalanche_daily_stablecoin_metrics_breakdown"),
                    ref("agg_polygon_daily_stablecoin_metrics_breakdown"),
                    ref("agg_ethereum_daily_stablecoin_metrics_breakdown"),
                    ref("agg_tron_daily_stablecoin_metrics_breakdown"),
                    ref("agg_bsc_daily_stablecoin_metrics_breakdown"),
                ]
            )
        }}
    )
select *, date::string || '-' || chain || '-' || symbol || from_address || contract_address as unique_id
from daily_data
where date < to_date(sysdate())