{{ config(materialized="table") }}

with
    sigma_apps as (
        select
            sigma.namespace,
            coalesce(updated_friendly_name, sigma.friendly_name) as friendly_name,
            coalesce(updated_sub_category, sigma.sub_category) as sub_category,
            coalesce(updated_category, sigma.category) as category,
            coalesce(updated_coingecko_id, sigma.coingecko_id) as coingecko_id,
            coalesce(updated_artemis_id, sigma.artemis_id) as artemis_id,
            sigma.ecosystem_id,
            sigma.defillama_protocol_id,
            coalesce(updated_parent_namespace, sigma.parent_namespace) as parent_app,
            coalesce(updated_visibility , sigma.visibility) as visibility,
            coalesce(sigma.update_symbol, sigma.symbol) as symbol
        from {{ ref("dim_apps_post_sigma") }} as sigma
        where sigma.namespace is not null
        union
        select
            namespace,
            friendly_name,
            sub_category,
            category,
            coingecko_id,
            artemis_id,
            ecosystem_id,
            defillama_protocol_id,
            parent_namespace as parent_app,
            true as visibility,
            symbol
        from {{ ref("dim_new_apps_post_sigma") }}
        where namespace is not null
    ),

    sigma_app_tagged as (
        select
            sigma_apps.namespace,
            max(sigma_apps.friendly_name) as friendly_name,
            max(sigma_apps.sub_category) as sub_category,
            max(sigma_apps.category) as category,
            max(sigma_apps.artemis_id) as artemis_id,
            max(sigma_apps.coingecko_id) as coingecko_id,
            max(sigma_apps.ecosystem_id) as ecosystem_id,
            max(sigma_apps.defillama_protocol_id) as defillama_protocol_id,
            max(sigma_apps.parent_app) as parent_app,
            max(sigma_apps.visibility) as visibility,
            max(coalesce(sigma_apps.symbol, token.token_symbol)) as symbol,
            max(token.token_image_small) as icon
        from sigma_apps
        left join
            {{ ref("dim_coingecko_tokens") }} as token
            on sigma_apps.coingecko_id = token.coingecko_token_id
        group by namespace
    ),
    all_apps as (
        select
            coalesce(
                sigma_app_tagged.namespace, sui.namespace, dune_namespace.namespace, flipside.namespace
            ) as namespace,
            coalesce(
                sigma_app_tagged.friendly_name,
                sui.friendly_name,
                initcap(replace(dune_namespace.namespace, '_', ' ')),
                flipside.friendly_name
            ) as friendly_name,
            coalesce(sigma_app_tagged.sub_category, sui.sub_category, flipside.sub_category) as sub_category,
            coalesce(sigma_app_tagged.category, sui.category, flipside.category) as category,
            artemis_id,
            coingecko_id,
            ecosystem_id,
            cast(defillama_protocol_id as string) as defillama_protocol_id,
            parent_app,
            coalesce(visibility, 1) as visibility,
            symbol,
            coalesce(sigma_app_tagged.icon, sui.icon) as icon
        from sigma_app_tagged
        full join
            {{ ref("dim_dune_namespaces") }} as dune_namespace
            on sigma_app_tagged.namespace = dune_namespace.namespace
        full join
            {{ ref("dim_flipside_namespaces") }} as flipside
            on sigma_app_tagged.namespace = flipside.namespace
        full join 
            {{ ref("dim_sui_namespaces") }} as sui
            on sigma_app_tagged.namespace = sui.namespace
        where
            sigma_app_tagged.namespace is not null
            or dune_namespace.namespace is not null
            or flipside.namespace is not null
            or sui.namespace is not null
    )
    SELECT
        namespace,
        friendly_name,
        case when sub_category = 'None' then null else sub_category end as sub_category,
        case when category = 'None' then null else category end as category,
        case when artemis_id = 'None' then null else artemis_id end as artemis_id, 
        coingecko_id,
        ecosystem_id,
        defillama_protocol_id,
        case when parent_app = 'None' then null else parent_app end as parent_app,
        visibility,
        symbol,
        icon
    FROM all_apps

