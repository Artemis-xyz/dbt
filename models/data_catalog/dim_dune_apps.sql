{{ config(materialized="table") }}

with
    defillama_parent as (
        select
            max(defillama_id) defillama_id,
            replace(replace(parentprotocol, '-', '_'), 'parent#', '') as cleaned_slug
        from artemis_replicated.postgres_public.core_pydefillamaprotocols
        group by cleaned_slug
    )
select
    null as artemis_id,
    cm.coingecko_token_id as coingecko_id,
    cm.token_symbol as symbol,
    null as ecosystem_id,
    is_visible as visibility,
    np.category,
    sub_category,
    namespace,
    friendly_name,
    parent_namespace_id as parent_app_id,
    np.id,
    coalesce(defi.defillama_id, defillama_parent.defillama_id) as defillama_protocol_id,
    cm.token_image_small as icon
from artemis_replicated.postgres_public.core_protocolnamespaces np
left join
    pc_dbt_db.prod.dim_coingecko_tokens as cm on np.coingecko_id = cm.coingecko_token_id
left join
    artemis_replicated.postgres_public.core_pydefillamaprotocols as defi
    on np.coingecko_id = defi.gecko_id
left join defillama_parent on np.namespace = defillama_parent.cleaned_slug
