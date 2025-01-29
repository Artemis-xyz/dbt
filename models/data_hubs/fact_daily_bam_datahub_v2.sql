{{ config(materialized="table") }}

with
    bam_data as (
        select
            date,
            null as app,
            null as friendly_name,
            case when category is null then 'unlabeled' else category end as category,
            case when sub_category is null then 'unlabeled' else sub_category end as sub_category,
            chain,
            total_gas as gas,
            total_gas_usd as gas_usd,
            transactions as txns,
            dau as daa,
            returning_users,
            new_users,
            low_sleep_users,
            high_sleep_users,
            sybil_users,
            non_sybil_users
        from {{ ref("all_chains_gas_dau_txns_by_subcategory") }} as bam_by_subcategory
        union
        select
            date,
            null as app,
            null as friendly_name,
            case when category is null then 'unlabeled' else category end as category,
            null as sub_category,
            chain,
            total_gas as gas,
            total_gas_usd as gas_usd,
            transactions as txns,
            dau as daa,
            returning_users,
            new_users,
            low_sleep_users,
            high_sleep_users,
            sybil_users,
            non_sybil_users
        from {{ ref("all_chains_gas_dau_txns_by_category_v2") }} as bam_by_category
        union
        select
            date,
            namespace as app,
            friendly_name,
            category,
            sub_category,
            chain,
            total_gas as gas,
            total_gas_usd as gas_usd,
            transactions as txns,
            dau as daa,
            returning_users,
            new_users,
            low_sleep_users,
            high_sleep_users,
            sybil_users,
            non_sybil_users
        from {{ ref("all_chains_gas_dau_txns_by_application") }} as bam_by_app
        where app is not null
        union
        select
            date,
            null as app,
            null as friendly_name,
            null as category,
            null as sub_category,
            chain,
            gas,
            gas_usd,
            txns,
            dau as daa,
            returning_users,
            new_users,
            low_sleep_users,
            high_sleep_users,
            sybil_users,
            non_sybil_users
        from {{ ref("all_chains_gas_dau_txns_by_chain") }} as bam_by_chain
        where chain = 'arbitrum'
    ),
    app_coingecko as (
        select
            apps.artemis_application_id as app,
            coalesce(apps.symbol, coingecko_tokens.token_symbol) as token_symbol
        from {{ ref("dim_all_apps_gold") }} as apps
        left join
            {{ ref("dim_coingecko_tokens") }} as coingecko_tokens
            on apps.coingecko_id = coingecko_tokens.coingecko_token_id
    )
-- Link Symbols with BAM Data
select
    concat(
        coalesce(cast(bam.app as string), '_this_is_null_'),
        '|',
        coalesce(cast(bam.category as string), '_this_is_null_'),
        '|',
        coalesce(cast(bam.sub_category as string), '_this_is_null_'),
        '|',
        coalesce(cast(bam.chain as string), '_this_is_null_')
        -- '|',
        -- coalesce(cast(bam.date as string), '_this_is_null_')
    ) as unique_id,
    bam.date,
    bam.app,
    bam.friendly_name,
    app_coingecko.token_symbol as app_symbol,
    bam.category,
    initcap(replace(bam.sub_category, '_', ' ')) as sub_category,
    lower(replace(bam.category, ' ', '_')) as category_symbol,
    bam.sub_category as sub_category_symbol,
    chain,
    coalesce(chains.symbol, 'all') as chain_symbol,
    bam.gas,
    bam.gas_usd,
    bam.txns,
    bam.daa,
    bam.new_users,
    bam.returning_users,
    bam.high_sleep_users,
    bam.low_sleep_users,
    bam.sybil_users,
    bam.non_sybil_users,
    concat(
        coalesce(
            app_coingecko.token_symbol,
            bam.app,
            lower(replace(bam.category, ' ', '_')),
            ''
        ),
        '-',
        coalesce(chain_symbol, chain, 'all')
    ) as excel_symbol
from bam_data as bam
left join app_coingecko on bam.app = app_coingecko.app
left join {{ ref("dim_chain") }} as chains on bam.chain = chains.artemis_id
