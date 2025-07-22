{{
    config(
        materialized="table",
        snowflake_warehouse="POLKADOT",
    )
}}


WITH latest_vesting_json AS (
  SELECT 
    PARSE_JSON(SOURCE_JSON) AS parsed_json
  FROM landing_database.prod_landing.raw_polkadot_vesting_by_day
  QUALIFY ROW_NUMBER() OVER (ORDER BY EXTRACTION_DATE DESC) = 1
),
coingecko_data AS (
    SELECT 
        date,
        SHIFTED_TOKEN_CIRCULATING_SUPPLY AS coingecko_circulating_supply
    FROM PC_DBT_DB.PROD.FACT_COINGECKO_TOKEN_DATE_ADJUSTED_GOLD
    WHERE coingecko_id = 'polkadot'
),
vesting_data AS (
  SELECT 
    TO_DATE(f.key::STRING) AS date,
    f.value:"totalVestedDot"::NUMBER AS vested,
    f.value:"dailyUnvestedDot"::NUMBER AS daily_unvested
  FROM latest_vesting_json,
       LATERAL FLATTEN(input => parsed_json) f
),
balances AS (
  SELECT
    TRY_CAST(parquet_raw:"block_date"::STRING AS DATE) AS Date,
    COALESCE(parquet_raw:"frozen", parquet_raw:"misc_frozen") AS frozen,
    parquet_raw:"reserved" AS reserved,
    parquet_raw:"free" AS free,
    parquet_raw:"accId" AS acc_id
  FROM LANDING_DATABASE.PROD_LANDING.RAW_POLKADOT_BALANCES_PARQUET
)

SELECT 
    b.Date,
    SUM(b.reserved) + SUM(b.free) AS max_supply_to_date,
    0 AS uncreated_tokens,
    SUM(b.reserved) + SUM(b.free) AS total_supply_to_date,
    0 AS cumulative_burned,
    
    SUM(
      CASE 
        WHEN b.acc_id = '13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB' 
        THEN COALESCE(b.free, 0) + COALESCE(b.reserved, 0) 
        ELSE 0 
      END
    ) AS foundation_wallet_balance,

    SUM(b.free) + SUM(b.reserved) -
    SUM(
      CASE 
        WHEN b.acc_id = '13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB' 
        THEN COALESCE(b.free, 0) + COALESCE(b.reserved, 0) 
        ELSE 0 
      END
    ) AS issued_supply,

    v.vested, 

    SUM(b.free) + SUM(b.reserved) -
    SUM(
      CASE 
        WHEN b.acc_id = '13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB' 
        THEN COALESCE(b.free, 0) + COALESCE(b.reserved, 0) 
        ELSE 0 
      END
    ) 
    - v.vested AS float,
    
    MAX(c.coingecko_circulating_supply) AS coingecko_circulating_supply

FROM balances b
LEFT JOIN vesting_data v
  ON b.Date = v.Date
LEFT JOIN coingecko_data c
    ON b.date = c.date
WHERE b.date > '2020-05-26'
GROUP BY b.Date, v.vested
ORDER BY b.Date