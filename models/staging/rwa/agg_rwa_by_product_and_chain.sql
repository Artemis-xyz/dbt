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
SELECT * FROM chain_metrics