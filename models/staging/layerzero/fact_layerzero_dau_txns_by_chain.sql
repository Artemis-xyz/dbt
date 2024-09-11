{{ config(
    materialized= "table",
    snowflake_warehouse="LAYERZERO"
) }}    

with agg as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_layerzero_avalanche_dau_txns"),   
                ref("fact_layerzero_arbitrum_dau_txns"),
                ref("fact_layerzero_base_dau_txns"),
                ref("fact_layerzero_blast_dau_txns"),
                ref("fact_layerzero_bsc_dau_txns"),
                ref("fact_layerzero_ethereum_dau_txns"),
                ref("fact_layerzero_optimism_dau_txns"),
                ref("fact_layerzero_polygon_dau_txns"),
                ref("fact_layerzero_gnosis_dau_txns"),
            ]
        )
    }}
)
SELECT
    date,
    chain,
    sum(dau) as dau,
    sum(txns) as txns
FROM agg
GROUP BY 1, 2