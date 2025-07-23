{{config(materialized="incremental", snowflake_warehouse="STANDARD_8D737A18", unique_key=["date_day", "chain_name", "chain_id", "exchange_name", "quote_asset"])}}
select 
    date as date_day
    , 'hypercore' as chain_name
    , 'hypercore:mainnet' as chain_id
    , 'hyperliquid' as exchange_name
    , case 
        when FEE_TOKEN = 'USDT0' and direction = 'Buy' then 'Quote assets for USDT'
        when FEE_TOKEN = 'USDT0' then 'USDT'
        when FEE_TOKEN = 'USDC' then 'USDC'
        when FEE_TOKEN in ('UBTC') then 'BTC'
        when FEE_TOKEN in ('UETH') then 'ETH'
        when FEE_TOKEN in ('USOL') then 'SOL'
        when FEE_TOKEN in ('USDE', 'USDHL', 'USDXL') then 'Other Stablecoins'
        else 'Other crypto'
    end as quote_asset
    , sum(price * size) as volume
    , count(distinct transaction_hash) as trades
from {{ref("fact_hyperliquid_trades")}}
{% if is_incremental() %}
    where date >= (select DATEADD('day', -3, max(date_day)) from {{ this }})
{% endif %}
group by 1, 2, 3, 4, 5      
