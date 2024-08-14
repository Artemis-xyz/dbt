{{
    config(
        materialized="table",
        snowflake_warehouse="USDC",
        database="usdc",
        schema="core",
        alias="ez_metrics",
    )
}}

select 
    symbol
    , date
    , sum(transfer_volume) as stablecoin_transfer_volume
    , sum(deduped_transfer_volume) as deduped_stablecoin_transfer_volume
    , sum(dau) as stablecoin_dau
    , sum(txns) as stablecoin_txns
    , sum(total_supply) as stablecoin_total_supply
from {{ ref("agg_daily_stablecoin_metrics") }}
where symbol = 'USDC'
group by symbol, date
order by date desc
