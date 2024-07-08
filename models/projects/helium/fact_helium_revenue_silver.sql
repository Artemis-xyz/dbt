{{ config(snowflake_warehouse="HELIUM") }}
WITH program AS (
    SELECT
        *,
        GET_PATH(inner_instruction, 'instructions[1]:parsed:info:amount') AS inst1_amount,
        GET_PATH(inner_instruction, 'instructions[3]:parsed:info:amount') AS inst3_amount,
        GET_PATH(inner_instruction, 'instructions[0]:parsed:info:amount') AS inst0_amount,
        GET_PATH(inner_instruction, 'instructions[2]:parsed:info:amount') AS inst2_amount,
        GET_PATH(inner_instruction, 'instructions[5]:parsed:info:amount') AS inst5_amount,
        GET_PATH(inner_instruction, 'instructions[7]:parsed:info:amount') AS inst7_amount
    FROM
        solana_flipside.core.fact_events
    WHERE
        program_id = 'credMBJhYFzfn7NxBMdU4aUqFggAjgztaCcv2Fo6fPT' AND
        SUCCEEDED = 'TRUE'
),
consolidated AS (
    SELECT
        block_timestamp,
        lower(tx_id) AS tx_id,
        CASE
            WHEN inst1_amount IS NOT NULL AND inst3_amount IS NOT NULL THEN inst1_amount::INT/1e8
            WHEN inst0_amount IS NOT NULL AND inst2_amount IS NOT NULL THEN inst0_amount::INT/1e8
            WHEN inst5_amount IS NOT NULL AND inst7_amount IS NOT NULL THEN inst5_amount::INT/1e8
            ELSE NULL
        END AS hnt_burned,
        CASE
            WHEN inst1_amount IS NOT NULL AND inst3_amount IS NOT NULL THEN inst3_amount::INT/1e5
            WHEN inst0_amount IS NOT NULL AND inst2_amount IS NOT NULL THEN inst2_amount::INT/1e5
            WHEN inst5_amount IS NOT NULL AND inst7_amount IS NOT NULL THEN inst7_amount::INT/1e5
            ELSE NULL
        END AS dc_minted
    FROM
        program
    WHERE
        (inst1_amount IS NOT NULL AND inst3_amount IS NOT NULL) OR
        (inst0_amount IS NOT NULL AND inst2_amount IS NOT NULL) OR
        (inst5_amount IS NOT NULL AND inst7_amount IS NOT NULL)
)
SELECT
    date(block_timestamp) AS date,
    SUM(hnt_burned) AS hnt_burned,
    SUM(dc_minted) AS revenue,
    'solana' AS chain,
    'helium' AS protocol
FROM
    consolidated
GROUP BY
    1
ORDER BY
    1 DESC
