{{
    config(
        materialized="table",
        snowflake_warehouse="RENZO_PROTOCOL",
    )
}}


SELECT
    DATE(BLOCK_TIMESTAMP) AS DATE
    ,SUM(AMOUNT_USD) AS FEES
    ,0.5 * SUM(AMOUNT_USD) AS REVENUE
    ,'renzo_protocol' AS APP
    ,'DeFi' AS CATEGORY
  FROM ethereum_flipside.core.ez_native_transfers
  WHERE
  lower(FROM_ADDRESS) = lower('0xf2F305D14DCD8aaef887E0428B3c9534795D0d60') and 
   lower(TO_ADDRESS) = lower('0xD22FB2d2c09C108c44b622c37F6d2f4Bc9f85668')
  GROUP BY
  DATE(block_timestamp)
  ORDER BY DATE(block_timestamp) DESC 


  