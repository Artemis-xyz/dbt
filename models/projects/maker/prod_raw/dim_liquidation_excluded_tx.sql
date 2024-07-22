{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="dim_liquidation_excluded_tx"
    )
}}



SELECT t.tx_hash
FROM ethereum_flipside.core.fact_traces t
JOIN {{ ref('dim_contracts') }} c
    ON t.from_address = c.contract_address
    AND c.contract_type IN ('FlapFlop')
GROUP BY t.tx_hash