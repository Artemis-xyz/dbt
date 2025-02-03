{{
    config(
        materialized="view",
        snowflake_warehouse="LAYERZERO",
        database="layerzero",
        schema="core",
        alias="ez_metrics",
    )
}}

with bridge_volume as (
    SELECT
        date,
        bridge_volume
    FROM {{ ref('fact_layerzero_bridge_volume_all_chains') }}
)

SELECT
    date,
    sum(fees) as fees,
    sum(bridge_daa) as bridge_daa,
    sum(bridge_txns) as bridge_txns,
    avg(bridge_volume) as bridge_volume
FROM {{ ref('ez_layerzero_metrics_by_chain') }}
LEFT JOIN bridge_volume using (date)
WHERE date < to_date(sysdate())
GROUP BY 1