{{ config(materialized="table") }}
with
    daily_data as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("agg_ethereum_daily_stablecoin_metrics_breakdown"),
                    ref("agg_base_daily_stablecoin_metrics_breakdown"),
                ]
            )
        }}
    )
select *, date::string || '-' || chain || '-' || symbol || from_address || contract_address as unique_id
from daily_data
where date < to_date(sysdate())