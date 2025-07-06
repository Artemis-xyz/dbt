
{{
    config(
        materialized="table",
        snowflake_warehouse="BITGET"
    )
}}

WITH burn_data AS (
  SELECT
    t.block_timestamp::date           AS date,
    SUM(t.raw_amount_precise) / 1e18  AS burn
  FROM ethereum_flipside.core.ez_token_transfers t
  WHERE LOWER(t.contract_address) = LOWER('0x54D2252757e1672EEaD234D27B1270728fF90581')
    AND t.to_address = '0x000000000000000000000000000000000000dead'
  GROUP BY 1
),

burn_cumulative AS (
  SELECT
    date,
    burn,
    SUM(burn) OVER (
      ORDER BY date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_burn
  FROM burn_data
),

supply_with_burn AS (
  SELECT
    s.date,
    s.max_supply,

    -- rolling cumulative burn over every supply date
    SUM(COALESCE(bc.burn, 0))
      OVER (
        ORDER BY s.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS cumulative_burn,

    -- subtract cumulative burn from max_supply
    s.max_supply
      - SUM(COALESCE(bc.burn, 0))
        OVER (ORDER BY s.date) AS total_supply,

    0 AS foundation_owned,

    -- issued = total minus foundation
    (s.max_supply
       - SUM(COALESCE(bc.burn, 0))
         OVER (ORDER BY s.date))
      - 0 AS issued_supply,

    s.bft_unvested,
    s.employee_incentive_unvested,
    s.ecosystem_unvested,
    s.promotion_unvested,
    s.commitment_unvested,
    s.protection_unvested,

    -- float_supply subtracts today's burn and foundation
    (
      s.bft_unvested
      + s.employee_incentive_unvested
      + s.promotion_unvested
      + s.commitment_unvested
      + s.protection_unvested
      - 0
      - COALESCE(bc.burn, 0)
    ) AS float_supply

  FROM pc_dbt_db.prod.bgb_daily_supply_data AS s
  LEFT JOIN burn_cumulative AS bc
    ON s.date = bc.date

  -- ← filter out any rows where date is NULL
  WHERE s.date IS NOT NULL
)

SELECT *
FROM supply_with_burn
ORDER BY date DESC

