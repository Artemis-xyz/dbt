{{ config(materialized="table") }}

select
    date,
    chain,
    txns,
    daa as dau,
    coalesce(gas, fees_native) as gas,
    coalesce(gas_usd, fees) as gas_usd,
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
    and category is null
    and app is null
    and (gas is not null or fees_native is not null)
union all
select
    date,
    chain,
    txns,
    daa as dau,
    gas,
    gas_usd,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
    null as sybil_users,
    null as non_sybil_users
from pc_dbt_db.prod.agg_daily_solana_fundamental_usage
