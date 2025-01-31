{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER"
    )
}}

SELECT
    date,
    aggregator_multi_hop_volume,
    aggregator_single_hop_volume,
    unique_aggregator_traders
FROM {{ ref("fact_jupiter_aggregator_volume") }}
FULL OUTER JOIN {{ ref("fact_jupiter_aggregator_unique_traders") }} USING (date)