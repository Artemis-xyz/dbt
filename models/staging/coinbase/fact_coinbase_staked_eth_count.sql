{{ config(materialized="table") }}
with cb_eth_market_cap as (
    select date, shifted_token_market_cap from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted
    where coingecko_id = 'coinbase-wrapped-staked-eth' and shifted_token_market_cap <> 0
),
eth_price as (
    select date, shifted_token_price_usd from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted
    where coingecko_id = 'ethereum'
),
eth_supply as (
    select cb_eth_market_cap.date, shifted_token_market_cap / shifted_token_price_usd as total_supply from 
    cb_eth_market_cap 
    JOIN eth_price
    on cb_eth_market_cap.date = eth_price.date
),
eth_supply_forward_filled as (
    {{ forward_fill('date', 'total_supply', 'eth_supply') }}
)
select
    eth_supply_forward_filled.date as date,
    coalesce(eth_supply_forward_filled.value, 0) as total_supply
from eth_supply_forward_filled