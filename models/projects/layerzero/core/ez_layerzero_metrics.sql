{{
    config(
        materialized="table",
        snowflake_warehouse="LAYERZERO",
        database="layerzero",
        schema="core",
        alias="ez_metrics",
    )
}}

with fees as (
    SELECT
        date,
        SUM(fees) as fees
    FROM {{ ref('fact_layerzero_fees_by_chain') }}
    GROUP BY 1
)
, dau_txns as (
    SELECT
        date,
        SUM(dau) as dau,
        SUM(txns) as txns
    FROM {{ ref('fact_layerzero_dau_txns_by_chain') }}
    GROUP BY 1
)

SELECT
    coalesce(f.date, d.date) as date,
    f.fees,
    d.dau,
    d.txns
FROM dau_txns d
LEFT JOIN fees f using (date)
WHERE coalesce(f.date, d.date) < to_date(sysdate())