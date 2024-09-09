{{ config(materialized="table") }}

with
    daily_data as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_across_flows"),
                    ref("fact_synapse_flows"),
                    ref("fact_wormhole_flows"),
                    ref("fact_arbitrum_one_bridge_flows"),
                    ref("fact_avalanche_bridge_flows"),
                    ref("fact_base_bridge_flows"),
                    ref("fact_optimism_bridge_flows"),
                    ref("fact_polygon_pos_bridge_flows"),
                    ref("fact_starknet_bridge_flows"),
                    ref("fact_zksync_era_bridge_flows"),
                    ref("fact_rainbow_bridge_flows"),
                ]
            )
        }}
    ), cosmos_ecosystem AS (
        -- Roll up all Cosmos chains into a single record for the whole ecosystem
        -- Get IBC flows from Injective
        SELECT
            date,
            'injective_ibc' AS app,
            'injective' AS source_chain,
            'cosmos_ecosystem' AS destination_chain,
            category,
            SUM(amount_usd) AS amount_usd,
            NULL AS fee_usd
        FROM {{ ref("fact_injective_bridge_flows") }}
        WHERE
            source_chain = 'injective'
            AND app = 'ibc'
        GROUP BY
            date, category

        UNION ALL

        -- Get IBC flows to Injective
        SELECT
            date,
            'injective_ibc' AS app,
            'cosmos_ecosystem' AS source_chain,
            'injective' AS destination_chain,
            category,
            SUM(amount_usd) AS amount_usd,
            NULL AS fee_usd
        FROM {{ ref("fact_injective_bridge_flows") }}
        WHERE
            destination_chain = 'injective'
            AND app = 'ibc'
        GROUP BY
            date, category

        UNION ALL

        -- Get flows to/from Injective from Peggy
        SELECT
            date,
            app,
            source_chain,
            destination_chain,
            category,
            amount_usd,
            NULL AS fee_usd
        FROM {{ ref("fact_injective_bridge_flows") }}
        WHERE
            app = 'peggy'
    ),
    unioned AS (
        SELECT * EXCLUDE _dbt_source_relation FROM daily_data
        UNION ALL
        SELECT * FROM cosmos_ecosystem
    )
select
    *,
    date::string
    || '-'
    || app
    || '-'
    || source_chain
    || '-'
    || destination_chain
    || '-'
    || category as unique_id
from unioned
where date < to_date(sysdate())
