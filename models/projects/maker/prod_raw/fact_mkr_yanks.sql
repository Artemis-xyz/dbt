{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_mkr_yanks"
    )
}}

WITH yanks_raw AS (
    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        _end,
        _id
    FROM {{ ref('fact_dssvesttransferrable_yank') }}
),

yanks_with_context AS (
    SELECT 
        y.*,
        c._bgn,
        c._tau,
        c.total_mkr,
        CASE 
            WHEN DATEADD(second, y._end, '1970-01-01'::timestamp) > y.ts 
            THEN DATEADD(second, y._end, '1970-01-01'::timestamp)
            ELSE y.ts 
        END AS end_time
    FROM yanks_raw y
    LEFT JOIN {{ ref('fact_mkr_vest_creates') }} c
        ON y._id = c.output_id
)

SELECT
    ts,
    hash,
    _id,
    TO_TIMESTAMP(CAST(_bgn AS VARCHAR)) AS begin_time,
    end_time,
    _tau,
    total_mkr AS original_total_mkr,
    (1 - (DATEDIFF(second, '1970-01-01'::timestamp, end_time) - _bgn) / _tau) * total_mkr AS yanked_mkr
FROM yanks_with_context