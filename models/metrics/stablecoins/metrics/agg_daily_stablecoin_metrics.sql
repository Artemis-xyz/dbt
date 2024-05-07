{{ config(materialized="table") }}
select
    date,
    total_supply,
    txns,
    dau,
    transfer_volume,
    deduped_transfer_volume,
    chain,
    symbol,
    contract_address,
    unique_id
from {{ ref("agg_daily_stablecoin_metrics_silver") }}
