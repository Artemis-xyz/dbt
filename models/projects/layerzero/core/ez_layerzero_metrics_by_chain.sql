{{
    config(
        materialized="table",
        snowflake_warehouse="LAYERZERO",
        database="layerzero",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with fees as (
    SELECT
        date,
        chain,
        fees
    FROM {{ ref('fact_layerzero_fees_by_chain') }}
)
, dau_txns as (
    SELECT
        date,
        chain,
        dau,
        txns
    FROM {{ ref('fact_layerzero_dau_txns_by_chain') }}
)

SELECT
    coalesce(f.date, d.date) as date,
    f.chain,
    f.fees,
    d.dau as bridge_daa,
    d.txns as bridge_txns
FROM dau_txns d
LEFT JOIN  fees f using (date, chain)
WHERE coalesce(f.date, d.date) < to_date(sysdate())