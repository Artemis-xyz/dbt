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
    d.dau,
    d.txns
FROM fees f
JOIN dau_txns d using (date, chain)
WHERE coalesce(f.date, d.date) < to_date(sysdate())