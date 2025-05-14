{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
    )
}}

with exchange_rates as (
    select 
        sy_address,
        exchange_rate as exchangeRate
    from {{ ref("fact_pendle_sy_info") }}
    where assetInfo_Type = 0
),

-- Calculate normalized values
normalized_amounts AS (
  SELECT
    date,
    chain,
    s.sy_address,
    -- Take the underlying asset address
    assetInfo_address AS asset_address,
    
    -- Calculate the normalized amount with proper decimal handling
    CASE
      -- For token-type SYs with exchange rates
      WHEN s.assetInfo_Type = 0 THEN
        -- Handle normal cases
        CASE
          -- Special cases for specific SY contracts (as in the original code)
          WHEN LOWER(s.sy_address) IN (
            '0x141ec2d606f12ff959d7d07cde6811e5fdff2831',
            '0xec30e55b51d9518cfcf5e870bcf89c73f5708f72',
            '0xd5cf704dc17403343965b4f9cd4d7b5e9b20cc52'
          ) THEN
            (s.total_Supply * POWER(10, (s.assetInfo_decimals - s.decimals)) * e.exchangeRate) / POWER(10, s.assetInfo_decimals)
          -- Handle vbnb case
          WHEN LOWER(s.sy_address) = '0x7b5a43070bd97c2814f0d8b3b31ed53450375c19' THEN
            (s.total_Supply * POWER(10, (s.assetInfo_decimals - 18)) * e.exchangeRate) / POWER(10, 18)
          -- Default case for token-type SYs
          ELSE
            (s.total_Supply * POWER(10, (s.assetInfo_decimals - s.decimals)) * e.exchangeRate) / POWER(10, 18)
        END
      -- For non-token-type SYs
      ELSE
        s.total_Supply * POWER(10, (s.assetInfo_decimals - s.decimals))
    END AS normalized_amount
    
  FROM {{ ref("fact_pendle_sy_info") }} s
  LEFT JOIN exchange_rates e ON LOWER(s.sy_address) = LOWER(e.sy_address)
)

-- Sum up the amounts by asset and prepare for price join
SELECT
  normalized_amounts.chain,
  normalized_amounts.sy_address,
  normalized_amounts.asset_address,
  normalized_amounts.date,
  normalized_amounts.normalized_amount,
  prices.price,
  prices.decimals,
  prices.symbol
FROM normalized_amounts
LEFT JOIN (
    {{ get_coingecko_prices_on_chains(['arbitrum', 'bsc', 'base', 'ethereum', 'optimism']) }}
) as prices
    on normalized_amounts.date = prices.date
    and (lower(normalized_amounts.asset_address) = lower(prices.contract_address)
    and normalized_amounts.chain = prices.chain)
    or (normalized_amounts.chain = prices.chain and normalized_amounts.asset_address = '0x0000000000000000000000000000000000000000' AND prices.contract_address like '%native')
-- GROUP BY 1, 2, 3