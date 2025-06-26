{{ config(materialized="table") }}

with
    bam_data as (
        select
            date,
            null as app,
            null as friendly_name,
            case when category is null then 'Unlabeled' else category end as category,
            chain,
            total_gas as gas,
            total_gas_usd as gas_usd,
            transactions as txns,
            dau as daa,
            contract_count,
            real_users,
            returning_users,
            new_users,
            low_sleep_users,
            high_sleep_users,
            sybil_users,
            non_sybil_users,
            avg_gas_per_address,
            avg_gas_usd_per_address,
            rev,
            rev_usd
        from {{ ref("all_chains_gas_dau_txns_by_category_v2") }} as bam_by_category
        union
        select
            date,
            namespace as app,
            friendly_name,
            category,
            chain,
            total_gas as gas,
            total_gas_usd as gas_usd,
            transactions as txns,
            dau as daa,
            contract_count,
            real_users,
            returning_users,
            new_users,
            low_sleep_users,
            high_sleep_users,
            sybil_users,
            non_sybil_users,
            avg_gas_per_address,
            avg_gas_usd_per_address,
            rev,
            rev_usd
        from {{ ref("all_chains_gas_dau_txns_by_application") }} as bam_by_app
        where app is not null
        union
        select
            date,
            null as app,
            null as friendly_name,
            null as category,
            chain,
            gas,
            gas_usd,
            txns,
            dau as daa,
            null as contract_count,
            null as real_users,
            returning_users,
            new_users,
            low_sleep_users,
            high_sleep_users,
            sybil_users,
            non_sybil_users,
            null as avg_gas_per_address,
            null as avg_gas_usd_per_address,
            null as rev,
            null as rev_usd
        from {{ ref("all_chains_gas_dau_txns_by_chain") }} as bam_by_chain
    ),
    app_coingecko as (
        select
            apps.artemis_application_id as app,
            coalesce(apps.symbol, coingecko_tokens.token_symbol) as token_symbol
        from {{ ref("dim_all_apps_gold") }} as apps
        left join
            {{ ref("dim_coingecko_tokens") }} as coingecko_tokens
            on apps.coingecko_id = coingecko_tokens.coingecko_token_id
    ),
    distinct_category_map AS (
        select distinct artemis_category_id, category_display_name
        from {{ ref("dim_category_datahub") }}
    )
-- Link Symbols with BAM Data
select
    concat(
        coalesce(cast(bam.app as string), '_this_is_null_'),
        '|',
        coalesce(cast(bam.category as string), '_this_is_null_'),
        '|',
        coalesce(cast(bam.chain as string), '_this_is_null_'),
        '|',
        coalesce(cast(bam.date as string), '_this_is_null_')
    ) as unique_id,
    bam.date,
    bam.app,
    bam.friendly_name,
    app_coingecko.token_symbol as app_symbol,
    coalesce(categories.category_display_name, bam.category)  as category,
    lower(bam.category) as category_symbol,
    chain,
    coalesce(chains.symbol, 'all') as chain_symbol,
    bam.gas,
    bam.gas_usd,
    bam.txns,
    bam.daa,
    bam.contract_count,
    bam.real_users,
    bam.new_users,
    bam.returning_users,
    bam.high_sleep_users,
    bam.low_sleep_users,
    bam.sybil_users,
    bam.non_sybil_users,
    bam.avg_gas_per_address,
    bam.avg_gas_usd_per_address,
    bam.rev,
    bam.rev_usd,
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
left join distinct_category_map as categories on bam.category = categories.artemis_category_id
