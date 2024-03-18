{{ config(materialized="table") }}
select
    dune.address,
    coalesce(sigma.override_namespace, dune.namespace) as namespace,
    dune.name,
    dune.chain
from {{ ref("dim_dune_contracts") }} as dune
left join
    {{ source("SIGMA", "sigma_tagged_dune_contracts") }} as sigma
    on dune.address = sigma.address
    and dune.chain = sigma.chain
    and dune.name = sigma.name
