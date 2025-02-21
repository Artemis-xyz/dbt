{{config(materialized="table", snowflake_warehouse='BAM_TRENDING_WAREHOUSE_LG')}}

with 
combined_events as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("dim_stargate_v2_arbitrum_fees"),
                ref("dim_stargate_v2_avalanche_fees"),
                ref("dim_stargate_v2_base_fees"),
                ref("dim_stargate_v2_ethereum_fees"),
                ref("dim_stargate_v2_optimism_fees"),
                ref("dim_stargate_v2_polygon_fees"),
            ],
        )
    }}
)
select
    date,
    tx_hash,
    contract_address,
    fees_native,
    avg_price,
    fees_usd,
    chain
from combined_events