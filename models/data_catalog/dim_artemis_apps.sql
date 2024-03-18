{{ config(materialized="table") }}

select
    artemis_id,
    ca.coingecko_id,
    ca.symbol,
    ecosystem_id,
    coalesce(n.is_visible, np.is_visible) as visibility,
    null as category,
    null as sub_category,
    coalesce(n.namespace, np.namespace) as namespace,
    coalesce(n.friendly_name, np.friendly_name) as friendly_name,
    coalesce(n.parent_namespace_id, np.parent_namespace_id) as parent_app_id,
    coalesce(n.id, np.id) id,
    llama.defillama_id as defillama_protocol_id
from artemis_replicated.postgres_public.core_asset as ca
left join
    artemis_replicated.postgres_public.core_protocolnamespaces as n
    on lower(ca.coingecko_id) = lower(n.coingecko_id)
left join
    artemis_replicated.postgres_public.core_protocolnamespaces as np
    on lower(ca.artemis_id) = lower(np.namespace)
left join
    artemis_replicated.postgres_public.core_pydefillamaprotocols llama
    on ca.defillama_protocol_id = llama.id
where defillama_protocol_id is not null and artemis_id not in ('base', 'osmosis')
