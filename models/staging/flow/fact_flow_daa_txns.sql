{{ config(materialized="view") }}
WITH ez_actors_data AS (
    SELECT
        date_trunc('day', b.block_timestamp) AS date,
        COUNT(DISTINCT CAST(value AS VARCHAR)) AS daa,  -- Explicitly casting to VARCHAR
        COUNT(DISTINCT b.tx_id) AS txns,
    FROM
        flow_flipside.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a  -- Flattening the actors array
    GROUP BY date
), evm_data AS (
    SELECT
        date_trunc('day', block_timestamp) AS date,
        COUNT(DISTINCT from_address) AS daa,
        COUNT(DISTINCT tx_hash) AS txns
    FROM flow_flipside.core_evm.fact_transactions
    GROUP BY date
)
SELECT
    date,
    SUM(daa) AS daa,
    SUM(txns) AS txns,
    'flow' AS chain
FROM (
    SELECT * FROM ez_actors_data
    UNION ALL
    SELECT * FROM evm_data
) AS combined_data
GROUP BY date
