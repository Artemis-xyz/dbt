{{
    config(
        materialized="view",
        snowflake_warehouse="LAYERZERO",
        database="layerzero",
        schema="core",
        alias="ez_metrics",
    )
}}


SELECT
    date,
    sum(fees) as fees,
    sum(bridge_daa) as bridge_daa,
    sum(bridge_txns) as bridge_txns
FROM {{ ref('ez_layerzero_metrics_by_chain') }}
WHERE date < to_date(sysdate())
GROUP BY 1