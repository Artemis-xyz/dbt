{{ config(
    materialized="incremental",
    snowflake_warehouse="APTOS_LG",
    unique_key=["transaction_hash", "event_index", "contract_address"]
) }}

WITH
token_meta AS (
  SELECT
      LOWER(token_address)        AS token_address,
      COALESCE(decimals, 6)       AS decimals,    
      symbol
  FROM aptos_flipside.core.dim_tokens   
),


deposit_events AS (
  SELECT
      block_number, tx_hash, block_timestamp,
      event_index AS receiving_event_index,
      account_address AS to_address,
      amount AS receiving_amount,
      token_address,
      ROW_NUMBER() OVER(PARTITION BY tx_hash, token_address ORDER BY event_index) AS rn
  FROM aptos_flipside.core.fact_transfers
  WHERE transfer_event = 'DepositEvent'
    AND amount > 0 AND amount < 1e18
    {% if is_incremental() %}
      AND block_timestamp > (SELECT MAX(block_timestamp) FROM {{ this }})
    {% endif %}
),
withdraw_events AS (
  SELECT
      block_number, tx_hash, block_timestamp,
      event_index AS withdraw_event_index,
      account_address AS from_address,
      amount AS withdraw_amount,
      token_address,
      ROW_NUMBER() OVER(PARTITION BY tx_hash, token_address ORDER BY event_index) AS rn
  FROM aptos_flipside.core.fact_transfers
  WHERE transfer_event = 'WithdrawEvent'
    AND amount > 0 AND amount < 1e18
    {% if is_incremental() %}
      AND block_timestamp > (SELECT MAX(block_timestamp) FROM {{ this }})
    {% endif %}
),
tx_event_counts AS (
  SELECT
      tx_hash,
      token_address,
      SUM(CASE WHEN transfer_event = 'DepositEvent' THEN 1 ELSE 0 END)  AS deposit_count,
      SUM(CASE WHEN transfer_event = 'WithdrawEvent' THEN 1 ELSE 0 END) AS withdraw_count
  FROM aptos_flipside.core.fact_transfers
  WHERE transfer_event IN ('DepositEvent','WithdrawEvent')
    AND amount > 0 AND amount < 1e18
    {% if is_incremental() %}
      AND block_timestamp > (SELECT MAX(block_timestamp) FROM {{ this }})
    {% endif %}
  GROUP BY tx_hash, token_address
),
case1_matches AS (
  SELECT
      w.tx_hash,
      COALESCE(w.block_number, d.block_number)      AS block_number,
      COALESCE(w.block_timestamp, d.block_timestamp) AS block_timestamp,
      COALESCE(w.token_address, d.token_address)    AS token_address,
      w.from_address,
      d.to_address,
      w.withdraw_event_index                        AS withdraw_event_index,
      d.receiving_event_index                       AS deposit_event_index,
      d.receiving_event_index                       AS event_index,
      w.withdraw_amount                             AS amount_raw,
      'transfer'                                    AS transfer_type
  FROM withdraw_events w
  JOIN deposit_events d
    ON w.tx_hash = d.tx_hash AND w.rn = d.rn AND w.token_address = d.token_address
  JOIN tx_event_counts c
    ON w.tx_hash = c.tx_hash AND w.token_address = c.token_address
  WHERE c.deposit_count = c.withdraw_count
    AND w.from_address != '0x0000000000000000000000000000000000000000000000000000000000000000'
    AND d.to_address   != '0x0000000000000000000000000000000000000000000000000000000000000000'
),
case2_matches AS (
  SELECT
      w.tx_hash,
      COALESCE(w.block_number, d.block_number)      AS block_number,
      COALESCE(w.block_timestamp, d.block_timestamp) AS block_timestamp,
      COALESCE(w.token_address, d.token_address)    AS token_address,
      w.withdraw_event_index,
      w.from_address,
      w.withdraw_amount,
      d.receiving_event_index,
      d.to_address,
      d.receiving_amount,
      ROW_NUMBER() OVER(
        PARTITION BY d.tx_hash, d.receiving_event_index, d.token_address
        ORDER BY ABS(d.receiving_amount - w.withdraw_amount),
                 ABS(d.receiving_event_index - w.withdraw_event_index)
      ) AS match_rank
  FROM withdraw_events w
  JOIN tx_event_counts c ON w.tx_hash = c.tx_hash AND w.token_address = c.token_address
  JOIN deposit_events d  ON w.tx_hash = d.tx_hash AND w.token_address = d.token_address
  WHERE c.deposit_count > c.withdraw_count
    AND w.from_address != '0x0000000000000000000000000000000000000000000000000000000000000000'
    AND d.to_address   != '0x0000000000000000000000000000000000000000000000000000000000000000'
),
case3_matches AS (
  SELECT
      d.tx_hash,
      COALESCE(d.block_number, w.block_number)      AS block_number,
      COALESCE(d.block_timestamp, w.block_timestamp) AS block_timestamp,
      COALESCE(d.token_address, w.token_address)    AS token_address,
      w.withdraw_event_index,
      w.from_address,
      w.withdraw_amount,
      d.receiving_event_index,
      d.to_address,
      d.receiving_amount,
      ROW_NUMBER() OVER(
        PARTITION BY w.tx_hash, w.withdraw_event_index, w.token_address
        ORDER BY ABS(d.receiving_amount - w.withdraw_amount),
                 ABS(d.receiving_event_index - w.withdraw_event_index)
      ) AS match_rank
  FROM deposit_events d
  JOIN tx_event_counts c ON d.tx_hash = c.tx_hash AND d.token_address = c.token_address
  JOIN withdraw_events w ON d.tx_hash = w.tx_hash AND d.token_address = w.token_address
  WHERE c.withdraw_count > c.deposit_count
    AND w.from_address != '0x0000000000000000000000000000000000000000000000000000000000000000'
    AND d.to_address   != '0x0000000000000000000000000000000000000000000000000000000000000000'
),
all_events AS (
  SELECT block_number, tx_hash, block_timestamp, event_index,
         account_address, amount, token_address, transfer_event
  FROM aptos_flipside.core.fact_transfers
  WHERE transfer_event IN ('DepositEvent','WithdrawEvent')
    AND amount > 0 AND amount < 1e18
    {% if is_incremental() %}
      AND block_timestamp > (SELECT MAX(block_timestamp) FROM {{ this }})
    {% endif %}
    AND tx_hash IN (
      SELECT tx_hash
      FROM aptos_flipside.core.fact_transfers
      WHERE transfer_event IN ('DepositEvent','WithdrawEvent')
        AND amount > 0 AND amount < 1e18
        {% if is_incremental() %}
          AND block_timestamp > (SELECT MAX(block_timestamp) FROM {{ this }})
        {% endif %}
      GROUP BY tx_hash, token_address
      HAVING COUNT_IF(transfer_event='WithdrawEvent') > 0
         AND COUNT_IF(transfer_event='DepositEvent')  > 0
    )
),
balance_stack AS (
  SELECT *,
         CASE WHEN transfer_event='WithdrawEvent' THEN -amount
              WHEN transfer_event='DepositEvent'  THEN  amount ELSE 0 END AS net_amount,
         SUM(CASE WHEN transfer_event='WithdrawEvent' THEN -amount
                  WHEN transfer_event='DepositEvent'  THEN  amount ELSE 0 END)
           OVER (PARTITION BY tx_hash, token_address ORDER BY event_index
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
  FROM all_events
),
session_boundaries AS (
  SELECT *,
         CASE WHEN LAG(running_balance,1,0)
                   OVER (PARTITION BY tx_hash, token_address ORDER BY event_index) < 0
                AND running_balance >= 0 THEN 1 ELSE 0 END AS session_start
  FROM balance_stack
),
sessions AS (
  SELECT *,
         SUM(session_start) OVER (PARTITION BY tx_hash, token_address ORDER BY event_index
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
  FROM session_boundaries
),
session_cumulatives AS (
  SELECT *,
         SUM(CASE WHEN transfer_event='WithdrawEvent' THEN amount ELSE 0 END)
           OVER (PARTITION BY tx_hash, token_address, session_id ORDER BY event_index
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS withdraw_cumulative,
         SUM(CASE WHEN transfer_event='DepositEvent' THEN amount ELSE 0 END)
           OVER (PARTITION BY tx_hash, token_address, session_id ORDER BY event_index
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS deposit_cumulative
  FROM sessions
),
with_previous_values AS (
  SELECT *,
         LAG(withdraw_cumulative,1,0) OVER (PARTITION BY tx_hash, token_address, session_id ORDER BY event_index) AS withdraw_cumulative_prev,
         LAG(deposit_cumulative,1,0)  OVER (PARTITION BY tx_hash, token_address, session_id ORDER BY event_index) AS deposit_cumulative_prev
  FROM session_cumulatives
),
stack_matched_transfers AS (
  SELECT
      w.block_number, w.block_timestamp, w.tx_hash, w.token_address,
      w.event_index AS withdraw_event_index,
      w.account_address AS from_address, w.amount AS withdraw_amount, w.withdraw_cumulative,
      d.event_index AS deposit_event_index,
      d.account_address AS to_address, d.amount AS deposit_amount, d.deposit_cumulative,
      LEAST(w.withdraw_cumulative - d.deposit_cumulative_prev,
            d.deposit_cumulative - w.withdraw_cumulative_prev) AS matched_amount,
      'transfer' AS transfer_type,
      ROW_NUMBER() OVER (PARTITION BY w.tx_hash, w.token_address, w.event_index ORDER BY d.event_index) AS match_rank
  FROM with_previous_values w
  JOIN with_previous_values d
    ON w.tx_hash = d.tx_hash
   AND w.token_address = d.token_address
   AND w.session_id = d.session_id
   AND w.transfer_event = 'WithdrawEvent'
   AND d.transfer_event = 'DepositEvent'
   AND w.event_index < d.event_index
   AND w.account_address != '0x0000000000000000000000000000000000000000000000000000000000000000'
   AND d.account_address != '0x0000000000000000000000000000000000000000000000000000000000000000'
  WHERE w.withdraw_cumulative > d.deposit_cumulative_prev
    AND d.deposit_cumulative > w.withdraw_cumulative_prev
),
complex_txs AS (
  SELECT DISTINCT tx_hash, token_address
  FROM all_events
  WHERE tx_hash NOT IN (
    SELECT DISTINCT tx_hash FROM case1_matches
    UNION SELECT DISTINCT tx_hash FROM case2_matches WHERE match_rank = 1
    UNION SELECT DISTINCT tx_hash FROM case3_matches WHERE match_rank = 1
  )
),
stack_matches AS (
  SELECT
      block_number, block_timestamp, tx_hash, token_address,
      withdraw_event_index AS event_index,
      from_address, to_address,
      matched_amount AS amount_raw,
      transfer_type
  FROM stack_matched_transfers
  WHERE match_rank = 1 AND matched_amount > 0
    AND (tx_hash, token_address) IN (SELECT tx_hash, token_address FROM complex_txs)
),

-- FIX: exclude complex txs here to prevent duplicates with stack_matches
unmatched_withdraws_simple AS (
  SELECT
      w.block_number, w.block_timestamp, w.tx_hash, w.token_address,
      w.withdraw_event_index AS event_index,
      w.from_address,
      '0x0000000000000000000000000000000000000000000000000000000000000000' AS to_address,
      w.withdraw_amount AS amount_raw,
      'potential_burn' AS transfer_type
  FROM withdraw_events w
  WHERE (w.tx_hash, w.token_address) NOT IN (SELECT tx_hash, token_address FROM complex_txs)
    AND NOT EXISTS (
      SELECT 1 FROM case1_matches c1
      WHERE w.tx_hash = c1.tx_hash AND w.token_address = c1.token_address AND w.withdraw_event_index = c1.withdraw_event_index
    )
    AND NOT EXISTS (
      SELECT 1 FROM case2_matches c2
      WHERE w.tx_hash = c2.tx_hash AND w.token_address = c2.token_address AND w.withdraw_event_index = c2.withdraw_event_index AND c2.match_rank = 1
    )
    AND NOT EXISTS (
      SELECT 1 FROM case3_matches c3
      WHERE w.tx_hash = c3.tx_hash AND w.token_address = c3.token_address AND w.withdraw_event_index = c3.withdraw_event_index AND c3.match_rank = 1
    )
),
unmatched_deposits_simple AS (
  SELECT
      d.block_number, d.block_timestamp, d.tx_hash, d.token_address,
      d.receiving_event_index AS event_index,
      '0x0000000000000000000000000000000000000000000000000000000000000000' AS from_address,
      d.to_address,
      d.receiving_amount AS amount_raw,
      'potential_mint' AS transfer_type
  FROM deposit_events d
  WHERE (d.tx_hash, d.token_address) NOT IN (SELECT tx_hash, token_address FROM complex_txs)
    AND NOT EXISTS (
      SELECT 1 FROM case1_matches c1
      WHERE d.tx_hash = c1.tx_hash AND d.token_address = c1.token_address AND d.receiving_event_index = c1.deposit_event_index
    )
    AND NOT EXISTS (
      SELECT 1 FROM case2_matches c2
      WHERE d.tx_hash = c2.tx_hash AND d.token_address = c2.token_address AND d.receiving_event_index = c2.receiving_event_index AND c2.match_rank = 1
    )
    AND NOT EXISTS (
      SELECT 1 FROM case3_matches c3
      WHERE d.tx_hash = c3.tx_hash AND d.token_address = c3.token_address AND d.receiving_event_index = c3.receiving_event_index AND c3.match_rank = 1
    )
),
unmatched_withdraws_stack AS (
  SELECT
      w.block_number, w.block_timestamp, w.tx_hash, w.token_address,
      w.event_index,
      w.account_address AS from_address,
      '0x0000000000000000000000000000000000000000000000000000000000000000' AS to_address,
      w.amount AS amount_raw,
      'potential_burn' AS transfer_type
  FROM session_cumulatives w
  WHERE w.transfer_event = 'WithdrawEvent'
    AND (w.tx_hash, w.token_address) IN (SELECT tx_hash, token_address FROM complex_txs)
    AND NOT EXISTS (
      SELECT 1 FROM stack_matches sm
      WHERE w.tx_hash = sm.tx_hash AND w.token_address = sm.token_address AND w.event_index = sm.event_index
    )
),
unmatched_deposits_stack AS (
  SELECT
      d.block_number, d.block_timestamp, d.tx_hash, d.token_address,
      d.event_index,
      '0x0000000000000000000000000000000000000000000000000000000000000000' AS from_address,
      d.account_address AS to_address,
      d.amount AS amount_raw,
      'potential_mint' AS transfer_type
  FROM session_cumulatives d
  WHERE d.transfer_event = 'DepositEvent'
    AND (d.tx_hash, d.token_address) IN (SELECT tx_hash, token_address FROM complex_txs)
    AND NOT EXISTS (
      SELECT 1 FROM stack_matches sm
      WHERE d.tx_hash = sm.tx_hash AND d.token_address = sm.token_address AND d.event_index = sm.event_index
    )
),
token_transfers AS (
  SELECT block_number, block_timestamp, tx_hash, token_address, event_index,
         from_address, to_address, amount_raw, transfer_type
  FROM case1_matches
  UNION ALL
  SELECT block_number, block_timestamp, tx_hash, token_address, receiving_event_index, from_address, to_address, receiving_amount, 'transfer'
  FROM case2_matches WHERE match_rank = 1
  UNION ALL
  SELECT block_number, block_timestamp, tx_hash, token_address, withdraw_event_index, from_address, to_address, withdraw_amount, 'transfer'
  FROM case3_matches WHERE match_rank = 1
  UNION ALL
  SELECT block_number, block_timestamp, tx_hash, token_address, event_index, from_address, to_address, amount_raw, transfer_type
  FROM stack_matches
  UNION ALL
  SELECT block_number, block_timestamp, tx_hash, token_address, event_index, from_address, to_address, amount_raw, transfer_type
  FROM unmatched_withdraws_simple
  UNION ALL
  SELECT block_number, block_timestamp, tx_hash, token_address, event_index, from_address, to_address, amount_raw, transfer_type
  FROM unmatched_deposits_simple
  UNION ALL
  SELECT block_number, block_timestamp, tx_hash, token_address, event_index, from_address, to_address, amount_raw, transfer_type
  FROM unmatched_withdraws_stack
  UNION ALL
  SELECT block_number, block_timestamp, tx_hash, token_address, event_index, from_address, to_address, amount_raw, transfer_type
  FROM unmatched_deposits_stack
),

-- FINAL de-dupe preference: keep 'transfer' over unmatched labels if any collision
ranked AS (
  SELECT
      t.*,
      ROW_NUMBER() OVER (
        PARTITION BY tx_hash, token_address, event_index
        ORDER BY CASE transfer_type WHEN 'transfer' THEN 0 ELSE 1 END, event_index
      ) AS rn
  FROM token_transfers t
)

SELECT
    r.block_number,
    r.block_timestamp,
    r.tx_hash AS transaction_hash,
    NULL      AS transaction_index,
    r.event_index,
    r.token_address AS contract_address,
    r.from_address,
    r.to_address,
    r.amount_raw,

    -- SCALE BY DECIMALS
    r.amount_raw / POWER(10, COALESCE(m.decimals, 6))                         AS amount_native,
    (r.amount_raw / POWER(10, COALESCE(m.decimals, 6))) * COALESCE(p.price,0) AS amount,
    COALESCE(p.price, 0)                                                      AS price,

    r.transfer_type
FROM ranked r
LEFT JOIN token_meta m
       ON LOWER(r.token_address) = m.token_address
LEFT JOIN aptos_flipside.price.ez_hourly_token_prices p
       ON DATE_TRUNC('hour', r.block_timestamp) = p.hour
      AND LOWER(r.token_address) = LOWER(p.token_address)
WHERE r.rn = 1