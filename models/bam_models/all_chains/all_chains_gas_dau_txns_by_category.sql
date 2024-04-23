{{ config(materialized="table") }}

select
    date,
    case when category = 'Unlabeled' then null else category end as category,
    chain,
    gas as total_gas,
    gas_usd as total_gas_usd,
    txns as transactions,
    daa as dau,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
    sybil_users,
    non_sybil_users
from {{ ref("fact_daily_asset_metrics_datahub") }}
where
    chain in (
        'arbitrum',
        'avalanche',
        'base',
        'bsc',
        'ethereum',
        'near',
        'optimism',
        'polygon',
        'solana',
        'sui',
        'tron'
    )
    and category is not null
    and app is null
    and txns is not null
