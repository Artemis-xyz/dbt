{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="STABLECOINS",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="dim_stablecoin_table_breakdown_precomputed",
        post_hook = "{{ merge_tags_dict({
            'duckdb': 'true',
            'order_by': 'stablecoin_supply, chain, symbol'
        }) }}"
    )
}}

SELECT
    * EXCLUDE(name, historical_l_30_stablecoin_supply),
    to_json(name) AS name,
    to_json(historical_l_30_stablecoin_supply) AS historical_l_30_stablecoin_supply
FROM PC_DBT_DB.PROD.dim_stablecoin_table_breakdown_precomputed
ORDER BY stablecoin_supply, chain, symbol