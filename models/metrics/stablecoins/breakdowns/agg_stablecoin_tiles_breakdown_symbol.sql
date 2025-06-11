{{ config(materialized="table", snowflake_warehouse="STABLECOIN_V2_LG_2") }}

with
max_date as (
    select max(date) as date
    from {{ ref("agg_daily_stablecoin_breakdown_with_labels_silver") }}
)

select
    symbol as breakdown
    , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
    , sum(artemis_stablecoin_transfer_volume) as artemis_stablecoin_transfer_volume
    , sum(stablecoin_daily_txns) as stablecoin_daily_txns
    , count(distinct case when stablecoin_transfer_volume > 0 then address end) as stablecoin_dau
    , sum(case when date = (select date from max_date) then stablecoin_supply end) as stablecoin_supply
from {{ ref("agg_daily_stablecoin_breakdown_with_labels_silver") }}
where date >= dateadd(day, -31, to_date(sysdate()))
group by symbol