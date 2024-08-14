{{ config(materialized="table", snowflake_warehouse="STABLECOIN_V2_LG") }}
select
    date
    , chain
    , symbol
    , count(distinct case when stablecoin_daily_txns > 0 then from_address end) as stablecoin_dau
    , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
    , sum(stablecoin_daily_txns) as stablecoin_daily_txns
    , count(distinct case when stablecoin_daily_txns > 0 then from_address end) as artemis_stablecoin_dau
    , sum(artemis_stablecoin_transfer_volume) as artemis_stablecoin_transfer_volume
    , sum(artemis_stablecoin_daily_txns) as artemis_stablecoin_daily_txns
    , count(distinct case when stablecoin_daily_txns > 0 then from_address end) as p2p_stablecoin_dau
    , sum(p2p_stablecoin_transfer_volume) as p2p_stablecoin_transfer_volume
    , sum(p2p_stablecoin_daily_txns) as p2p_stablecoin_daily_txns
    , sum(stablecoin_supply) as stablecoin_supply
    , sum(case when is_wallet::number = 1 then stablecoin_supply else 0 end) as p2p_stablecoin_supply
from {{ ref("agg_daily_stablecoin_breakdown_silver") }}
group by date, symbol, chain
order by date