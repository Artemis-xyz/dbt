{{ config(materialized="table") }}

with
    parent_apps as (
        select distinct namespace as app_namespace
        from {{ ref("all_chains_gas_dau_txns_by_namespace") }}
        union
        select distinct namespace as app_namespace
        from {{ ref("dim_apps_gold") }}
        where artemis_id is not null
    ),
    asset_table as (
        select
            parent_apps.app_namespace,
            friendly_name as app_name,
            null as chain_name,
            artemis_id,
            coingecko_id,
            ecosystem_id,
            defillama_protocol_id,
            null as defillama_chain_name,
            visibility,
            symbol as app_symbol,
            icon as app_icon,
            null as chain_symbol,
            null as chain_display_name,
            category,
            null as category_display_name,
            sub_category
        from parent_apps
        left join
            {{ ref("dim_apps_gold") }} as app_gold
            on parent_apps.app_namespace = app_gold.namespace
        union
        select
            null as app_namespace,
            null as app_name,
            artemis_id as chain_name,
            artemis_id,
            coingecko_id,
            ecosystem_id,
            null as defillama_protocol_id,
            defillama_chain_name,
            visibility,
            null as app_symbol,
            null as app_icon,
            symbol as chain_symbol,
            name as chain_display_name,
            null as category,
            null as category_display_name,
            null as sub_category
        from {{ ref("dim_chain") }}
        union
        select
            null as app_namespace,
            null as app_name,
            null as chain_name,
            null as artemis_id,
            null as coingecko_id,
            null as ecosystem_id,
            null as defillama_protocol_id,
            null as defillama_chain_name,
            1 as visibility,
            null as app_symbol,
            null as app_icon,
            null as chain_symbol,
            null as chain_display_name,
            category,
            category_display_name,
            null as sub_category
        from {{ ref("dim_category") }}
    )
select
    concat(
        coalesce(cast(app_namespace as string), '_this_is_null_'),
        '|',
        coalesce(cast(category as string), '_this_is_null_'),
        '|',
        coalesce(cast(chain_name as string), '_this_is_null_')
    ) as unique_id,
    app_namespace,
    app_name,
    chain_name,
    artemis_id,
    coingecko_id,
    ecosystem_id,
    defillama_protocol_id,
    defillama_chain_name,
    visibility,
    app_symbol,
    app_icon,
    chain_symbol,
    chain_display_name,
    category as category_name,
    category_display_name,
    sub_category
from asset_table
where visibility = 1
