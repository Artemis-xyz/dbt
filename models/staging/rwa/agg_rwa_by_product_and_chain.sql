{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

with
    chain_metrics as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_arbitrum_rwa_supply_by_date_and_chain"),
                    ref("fact_avalanche_rwa_supply_by_date_and_chain"),
                    ref("fact_base_rwa_supply_by_date_and_chain"),
                    ref("fact_mantle_rwa_supply_by_date_and_chain"),
                    ref("fact_optimism_rwa_supply_by_date_and_chain"),
                    ref("fact_polygon_rwa_supply_by_date_and_chain"),
                    ref("fact_ethereum_rwa_supply_by_date_and_chain"),
                    ref("fact_solana_rwa_supply_by_date_and_chain"),
                ]
            )
        }}
    )
SELECT
    date
    , chain
    , i.symbol
    , i.issuer_id
    , i.issuer_friendly_name
    , i.product_type
    , price
    , net_rwa_supply_native_change
    , net_rwa_supply_usd_change
    , rwa_supply_native
    , rwa_supply_usd
FROM chain_metrics
left join {{ ref( "dim_rwa_product_issuer_and_type") }} i
    on chain_metrics.symbol = i.symbol