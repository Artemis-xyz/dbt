{{ config(materialized="table") }}

-- Step 1: Define the date spine
WITH wallet_categories AS (
  SELECT * FROM VALUES
    ('0.0.70', 'unvested_tokens'), --Employee & Founder Vesting
    ('0.0.72', 'unvested_tokens'), --SAFT
    ('0.0.74', 'unvested_tokens'), --SAFT
    ('0.0.76', 'unvested_tokens')  --SAFT
    AS wallet(address, category)
),

tagged_balances AS (
  SELECT
    b.snapshot_date,
    b.account,
    b.balance,
    COALESCE(w.category, 'foundation_balance') AS category
  FROM pc_dbt_db.prod.fact_hedera_account_balances b
  LEFT JOIN wallet_categories w
    ON b.account = w.address
),

aggregated AS (
  SELECT
    snapshot_date AS date,
    50000000000 AS max_supply,
    0 as uncreated_tokens,
    max_supply - uncreated_tokens as total_supply,
    0 as cumulative_burned_hbar,
    SUM(CASE WHEN category = 'foundation_balance' THEN balance ELSE 0 END) AS foundation_balances,
    total_supply - cumulative_burned_hbar - foundation_balances AS issued_supply,
    SUM(CASE WHEN category = 'unvested_tokens' THEN balance ELSE 0 END) AS unvested_balances,
    issued_supply - unvested_balances AS circulating_supply_native
  FROM tagged_balances
  GROUP BY snapshot_date
)

SELECT * FROM aggregated
ORDER BY date