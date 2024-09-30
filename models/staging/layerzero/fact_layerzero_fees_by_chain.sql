{{ config(  
    materialized= "table",
    snowflake_warehouse="LAYERZERO"
) }}

with agg as (
    {{
        dbt_utils.union_relations(
            relations = [
                ref('fact_layerzero_arbitrum_executor_fees'),
                ref('fact_layerzero_arbitrum_dvn_fees'),
                ref('fact_layerzero_avalanche_executor_fees'),
                ref('fact_layerzero_avalanche_dvn_fees'),
                ref('fact_layerzero_base_executor_fees'),
                ref('fact_layerzero_base_dvn_fees'),
                ref('fact_layerzero_bsc_executor_fees'),
                ref('fact_layerzero_bsc_dvn_fees'),
                ref('fact_layerzero_blast_executor_fees'),
                ref('fact_layerzero_blast_dvn_fees'),
                ref('fact_layerzero_ethereum_executor_fees'),
                ref('fact_layerzero_ethereum_dvn_fees'),
                ref('fact_layerzero_optimism_executor_fees'),
                ref('fact_layerzero_optimism_dvn_fees'),
                ref('fact_layerzero_polygon_executor_fees'),
                ref('fact_layerzero_polygon_dvn_fees'),
                ref('fact_layerzero_gnosis_executor_fees'),
                ref('fact_layerzero_gnosis_dvn_fees'),
            ]
        )
    }}
)
SELECT
    DATE,
    CHAIN,
    SUM(TOTAL_FEES_USD) AS fees
FROM agg
GROUP BY 1,2