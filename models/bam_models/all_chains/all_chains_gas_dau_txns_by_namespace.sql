{{ config(materialized="table") }}


with
    namespace_data as (
        select
            date,
            app as namespace,
            friendly_name,
            category,
            chain,
            txns as transactions,
            daa as dau,
            gas as total_gas,
            gas_usd as total_gas_usd,
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
                'tron'
            )
            and app is not null
            and txns is not null
    ),
    namespace_coingecko as (
        select
            namespace,
            token_image_thumb as image_thumbnail,
            token_image_small as image_small,
            cg.coingecko_token_id as coingecko_id
        from {{ ref("dim_apps_gold") }} pn
        left join
            {{ ref("dim_coingecko_tokens") }} cg
            on pn.coingecko_id = cg.coingecko_token_id
    )
select
    date,
    ns.namespace,
    friendly_name,
    category,
    chain,
    image_thumbnail,
    image_small,
    coingecko_id,
    total_gas,
    total_gas_usd,
    transactions,
    dau,
    returning_users,
    new_users,
    high_sleep_users,
    low_sleep_users,
    sybil_users,
    non_sybil_users
from namespace_data ns
left join namespace_coingecko cg on ns.namespace = cg.namespace
