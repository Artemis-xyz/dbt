{{config(materialized="incremental", snowflake_warehouse="STANDARD_8D737A18", unique_key=["date_day", "chain_name", "chain_id", "exchange_name", "quote_asset"])}}

with all_dex as (

SELECT 
    block_timestamp::date as date_day,
    'base' as chain,
    platform as dex,
    CASE 
      -- First identify USDT quote pairs (where USDT is being priced in something else)
      WHEN symbol_in = 'USDT' THEN 'Quote assets for USDT'
      WHEN symbol_out = 'USDT' THEN 'USDT'
      -- Then handle other major assets
      WHEN symbol_in IN ('USDC') THEN 'USDC'
      WHEN symbol_in IN ('WBTC', 'BTC', 'cbBTC') then 'WBTC'
      WHEN symbol_in IN ('WETH', 'ETH') then 'WETH'
      WHEN symbol_in IN ('SOL') then 'SOL'
      when symbol_in in ('USDe', 'USDS', 'DAI', 'FDUSD', 'BUIDL', 'USDTB', 'USD0', 'PYUSD', 'sUSD', 'USDY') then 'Other Stablecoins'
      ELSE 'Other crypto'
    END as quote_asset,
    sum(coalesce(amount_out_usd, amount_in_usd)) as volume,
    count(distinct tx_hash) as trades
  FROM base_flipside.defi.ez_dex_swaps
  --Filter out High volume MEV address: 0xf14149bde6f7e2573f38aceda6220d7dfff66592
  where origin_from_address not in ('0xf14149bde6f7e2573f38aceda6220d7dfff66592')
  {% if is_incremental() %}
    where block_timestamp >= (select DATEADD('day', -3, max(date_day)) from {{ this }})
  {% endif %}
  group by 1,2,3,4

  union all

  SELECT 
    block_timestamp::date as date_day,
    'ethereum' as chain,
    platform as dex,
    CASE 
      -- First identify USDT quote pairs (where USDT is being priced in something else)
      WHEN symbol_in = 'USDT' THEN 'Quote assets for USDT'
      WHEN symbol_out = 'USDT' THEN 'USDT'
      -- Then handle other major assets
      WHEN symbol_in IN ('USDC') THEN 'USDC'
      WHEN symbol_in IN ('WBTC', 'BTC', 'cbBTC') then 'WBTC'
      WHEN symbol_in IN ('WETH', 'ETH') then 'WETH'
      WHEN symbol_in IN ('SOL') then 'SOL'
      when symbol_in in ('USDe', 'USDS', 'DAI', 'FDUSD', 'BUIDL', 'USDTB', 'USD0', 'PYUSD', 'sUSD', 'USDY') then 'Other Stablecoins'
      ELSE 'Other crypto'
    END as quote_asset,
    sum(coalesce(amount_out_usd, amount_in_usd)) as volume,
    count(distinct tx_hash) as trades
  FROM ethereum_flipside.defi.ez_dex_swaps
  {% if is_incremental() %}
    where block_timestamp >= (select DATEADD('day', -3, max(date_day)) from {{ this }})
  {% endif %}
  group by 1,2,3,4

  union all

  SELECT 
    block_timestamp::date as date_day,
    'arbitrum' as chain,
        platform as dex,
    CASE 
      -- First identify USDT quote pairs (where USDT is being priced in something else)
      WHEN symbol_in = 'USDT' THEN 'Quote assets for USDT'
      WHEN symbol_out = 'USDT' THEN 'USDT'
      -- Then handle other major assets
      WHEN symbol_in IN ('USDC') THEN 'USDC'
      WHEN symbol_in IN ('WBTC', 'BTC', 'cbBTC') then 'WBTC'
      WHEN symbol_in IN ('WETH', 'ETH') then 'WETH'
      WHEN symbol_in IN ('SOL') then 'SOL'
      when symbol_in in ('USDe', 'USDS', 'DAI', 'FDUSD', 'BUIDL', 'USDTB', 'USD0', 'PYUSD', 'sUSD', 'USDY') then 'Other Stablecoins'
      ELSE 'Other crypto'
    END as quote_asset,
    sum(coalesce(amount_out_usd, amount_in_usd)) as volume,
    count(distinct tx_hash) as trades
  FROM arbitrum_flipside.defi.ez_dex_swaps
  {% if is_incremental() %}
    where block_timestamp >= (select DATEADD('day', -3, max(date_day)) from {{ this }})
  {% endif %}
  group by 1,2,3,4



  union all

  SELECT 
    block_timestamp::date as date_day,
    'bsc' as chain,
    platform as dex,
    CASE 
      -- First identify USDT quote pairs (where USDT is being priced in something else)
      WHEN symbol_in = 'USDT' THEN 'Quote assets for USDT'
      WHEN symbol_out = 'USDT' THEN 'USDT'
      -- Then handle other major assets
      WHEN symbol_in IN ('USDC') THEN 'USDC'
      WHEN symbol_in IN ('WBTC', 'BTC', 'cbBTC') then 'WBTC'
      WHEN symbol_in IN ('WETH', 'ETH') then 'WETH'
      WHEN symbol_in IN ('SOL') then 'SOL'
      when symbol_in in ('USDe', 'USDS', 'DAI', 'FDUSD', 'BUIDL', 'USDTB', 'USD0', 'PYUSD', 'sUSD', 'USDY') then 'Other Stablecoins'
      ELSE 'Other crypto'
    END as quote_asset,
    sum(coalesce(amount_out_usd, amount_in_usd)) as volume,
    count(distinct tx_hash) as trades
  FROM bsc_flipside.defi.ez_dex_swaps
  {% if is_incremental() %}
    where block_timestamp >= (select DATEADD('day', -3, max(date_day)) from {{ this }})
  {% endif %}
  group by 1,2,3,4

  union all

  SELECT 
    block_timestamp::date as date_day,
    'solana' as chain,
    swap_program as dex,
    CASE 
      -- First identify USDT quote pairs (where USDT is being priced in something else)
      WHEN swap_to_symbol = 'USDT' THEN 'Quote assets for USDT'
      WHEN swap_from_symbol = 'USDT' THEN 'USDT'
      -- Then handle other major assets
      WHEN swap_from_symbol IN ('USDC') THEN 'USDC'
      WHEN swap_from_symbol IN ('WBTC', 'BTC', 'cbBTC') then 'WBTC'
      WHEN swap_from_symbol IN ('WETH', 'ETH') then 'WETH'
      WHEN swap_from_symbol IN ('SOL') then 'SOL'
      when swap_from_symbol in ('USDe', 'USDS', 'DAI', 'FDUSD', 'BUIDL', 'USDTB', 'USD0', 'PYUSD', 'sUSD', 'USDY') then 'Other Stablecoins'
      ELSE 'Other crypto'
    END as quote_asset,
    sum(coalesce(swap_from_amount_usd, swap_to_amount_usd)) as volume,
    count(distinct tx_id) as trades
  FROM solana_flipside.defi.ez_dex_swaps
  {% if is_incremental() %}
    where block_timestamp >= (select DATEADD('day', -3, max(date_day)) from {{ this }})
  {% endif %}
  group by 1,2,3,4
  )

  select 
    date_day
    , chain as chain_name
    , case when chain like 'ethereum' then 1
        when chain like 'bsc' then 56
        when chain like 'base' then 8453
        when chain like 'arbitrum' then 42161
        when chain like 'solana' then 900
    end as chain_id
    , dex as exchange_name
    , quote_asset
    , volume as quote_volume_usd
    , trades
  from all_dex
  where volume < 10000000000