with
    existing as (
        select old.address, old.name, pn.namespace, chain.artemis_id
        from {{ source("POSTGRES_REPLICATED", "core_usersubmittedcontracts") }} as old
        left join {{ ref("dim_chain") }} as chain on old.asset_link_id = chain.id
        left join
            {{ source("POSTGRES_REPLICATED", "core_protocolnamespaces") }} as pn
            on old.namespace_link_id = pn.id
    )
select
    coalesce(sigma.address, existing.address) as address,
    coalesce(sigma.name, existing.name) as name,
    coalesce(sigma.chain, existing.artemis_id) as chain,
    coalesce(sigma.app, existing.namespace) as namespace
from {{ source("SIGMA", "sigma_contracts_to_apps") }} as sigma
full join
    existing on sigma.address = existing.address and sigma.chain = existing.artemis_id
where sigma.address is not null or existing.address is not null
