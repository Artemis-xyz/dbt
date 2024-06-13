{{ config(snowflake_warehouse="HELIUM") }}
with events as(
    SELECT
        *
    FROM
      solana_flipside.core.fact_events e
    WHERE program_id = 'hemjuPXBpNvggtaUnN1MwT3wrdhttKEfosTcc2P9Pg8'
    AND SUCCEEDED = 'TRUE'
    AND (
         GET_PATH(inner_instruction, 'instructions[3]:parsed:info:amount') IS NOT NULL OR
         GET_PATH(inner_instruction, 'instructions[5]:parsed:info:amount') IS NOT NULL OR
         GET_PATH(inner_instruction, 'instructions[7]:parsed:info:amount') IS NOT NULL
         )
    ORDER BY
      block_timestamp DESC
),
all_burns AS(
    SELECT
        block_timestamp,
        tx_id,
        CASE
        WHEN GET_PATH(inner_instruction, 'instructions[3]:parsed:info:amount') IS NOT NULL -- 3.4
          THEN CAST(GET_PATH(inner_instruction, 'instructions[3]:parsed:info:amount') AS NUMBER)
        WHEN GET_PATH(inner_instruction, 'instructions[5]:parsed:info:amount') IS NOT NULL  -- 3.6
          THEN CAST(GET_PATH(inner_instruction, 'instructions[5]:parsed:info:amount') AS NUMBER)
        WHEN GET_PATH(inner_instruction, 'instructions[7]:parsed:info:amount') IS NOT NULL  -- 3.8 (very rare)
          THEN CAST(GET_PATH(inner_instruction, 'instructions[7]:parsed:info:amount') AS NUMBER)
        END AS dc_burned
    FROM
      events
)
SELECT
  DATE(block_timestamp) as date,
  SUM(dc_burned)*1e-5 as onboard_fees,
  'solana' AS chain,
  'helium' AS protocol
FROM all_burns
where date < to_date(sysdate())
GROUP BY 1
ORDER BY 1 desc
