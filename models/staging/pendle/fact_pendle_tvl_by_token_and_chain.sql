{{
    config( 
        materialized="table",
        snowflake_warehouse="PENDLE"
    )
}}

{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
    )
}}

with exchange_rates as (
    select 
        date,
        sy_address,
        exchange_rate as exchangeRate
    from {{ ref("fact_pendle_sy_info") }}
    where assetInfo_Type = 0
),

-- Calculate normalized values
normalized_amounts AS (
  SELECT
    s.date,
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
  LEFT JOIN exchange_rates e ON LOWER(s.sy_address) = LOWER(e.sy_address) AND e.date = s.date
)
, prices as (
    {{ get_coingecko_prices_on_chains(['arbitrum', 'bsc', 'base', 'ethereum', 'optimism']) }}
)

-- Sum up the amounts by asset and prepare for price join

SELECT
    na.date,
    na.chain,
    na.sy_address,
    na.asset_address,
    na.normalized_amount / POWER(10, COALESCE(p_regular.decimals, p_native.decimals)) as tvl_native,
    COALESCE(p_regular.price, p_native.price) AS price,
    na.normalized_amount * COALESCE(p_regular.price, p_native.price) / POWER(10, COALESCE(p_regular.decimals, p_native.decimals)) AS tvl_usd,
    COALESCE(p_regular.decimals, p_native.decimals) AS decimals,
    COALESCE(p_regular.symbol, p_native.symbol) AS symbol
FROM normalized_amounts na
-- Regular token join
LEFT JOIN prices p_regular
    ON na.date = p_regular.date
    AND LOWER(na.asset_address) = LOWER(p_regular.contract_address)
    AND na.chain = p_regular.chain
-- Native token join (only applied when asset_address is zero address)
LEFT JOIN prices p_native
    ON na.date = p_native.date
    AND na.chain = p_native.chain
    AND na.asset_address = '0x0000000000000000000000000000000000000000'
    AND p_native.contract_address LIKE '%native'
WHERE COALESCE(p_regular.price, p_native.price) IS NOT NULL
