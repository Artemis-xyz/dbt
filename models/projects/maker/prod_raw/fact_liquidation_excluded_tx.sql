{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_liquidation_excluded_tx"
    )
}}

SELECT DISTINCT t.tx_hash
FROM ethereum_flipside.core.fact_traces t
JOIN {{ ref('dim_maker_contracts') }} c
    ON t.from_address = c.contract_address
    AND c.contract_type IN ('FlapFlop')