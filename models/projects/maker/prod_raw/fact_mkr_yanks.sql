{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_mkr_yanks"
    )
}}

WITH yanks_with_context AS (
    SELECT 
        y.block_timestamp AS ts,
        y.tx_hash AS hash,
        y._end,
        y._id,
        c._bgn,
        c._tau,
        c.total_mkr,
        CASE 
            WHEN FROM_UNIXTIME(CAST(y._end AS DOUBLE)) > y.block_timestamp 
            THEN FROM_UNIXTIME(CAST(y._end AS DOUBLE)) 
            ELSE y.block_timestamp 
        END AS end_time
    FROM {{ ref('fact_dssvesttransferrable_yank') }} y
    LEFT JOIN {{ ref('fact_mkr_vest_creates') }} c
        ON y._id = c.output_id
    -- Note: In the future, add a condition for call_success when available
)

SELECT
    ts,
    hash,
    _id,
    FROM_UNIXTIME(CAST(_bgn AS DOUBLE)) AS begin_time,
    end_time,
    _tau,
    total_mkr AS original_total_mkr,
    (1 - (UNIX_TIMESTAMP(end_time) - _bgn * 1e0) / _tau) * total_mkr AS yanked_mkr
FROM yanks_with_context