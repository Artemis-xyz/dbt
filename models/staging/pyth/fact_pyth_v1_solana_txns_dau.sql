{{
    config(
        materialized='incremental',
        unique_key='date',
        snowflake_warehouse='PYTH',
    )
}}

--  This data model is only for the v1 of the pyth protocol (only on Solana)

WITH events_data AS (
    SELECT
        e.block_timestamp::date AS date,
        e.tx_id,
        e.index, -- To differentiate multiple Pyth protocol events within a single transaction
        e.program_id,
        flattened_signers.value AS signer -- Access the `value` field from FLATTEN
    FROM
        solana_flipside.core.fact_events e,
        LATERAL FLATTEN(input => e.signers) AS flattened_signers -- Alias for the FLATTEN operation
    where program_id = 'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH'
    {% if is_incremental() %}
        and block_timestamp > (select max(date) from {{ this }})
    {% else %}
        and block_timestamp > '2021-08-16'
    {% endif %}
)

SELECT
    date,
    COUNT(DISTINCT tx_id || ':' || index) AS txns, -- Count unique txn + event pairs
    COUNT(DISTINCT signer) AS dau -- Count unique users
FROM
    events_data
WHERE
    date < to_date(sysdate())
GROUP BY
    date
ORDER BY
    date