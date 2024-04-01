{{ config(materialized="table") }}
with
    existing_chains as (
        select
            dim_chains.id,
            artemis_id,
            coingecko_id,
            lower(defillama_chains.name) as defillama_chain_name,
            ecosystem_id,
            symbol,
            dim_chains.name,
            category_id,
            sub_category_id,
            visibility
        from {{ source("MANUAL_STATIC_TABLES", "dim_chains") }} as dim_chains
        left join
            {{ source("POSTGRES_REPLICATED", "core_pydefillamachains") }}
            as defillama_chains
            on dim_chains.defillama_chain_id = defillama_chains.id
    )
select
    coalesce(sigma.id, existing.id) as id,
    coalesce(null, existing.category_id) as category_id,
    coalesce(null, existing.sub_category_id) as sub_category_id,
    coalesce(sigma.coingecko_id, existing.coingecko_id) as coingecko_id,
    coalesce(sigma.chain_id, existing.artemis_id) as artemis_id,
    lower(
        coalesce(sigma.defillama_chain_name, existing.defillama_chain_name)
    ) as defillama_chain_name,
    coalesce(sigma.ecosystem_id, existing.ecosystem_id) as ecosystem_id,
    lower(coalesce(sigma.symbol, existing.symbol)) as symbol,
    coalesce(1, existing.visibility) as visibility,
    coalesce(sigma.display_name, existing.name) as name
from existing_chains as existing
full join
    (
        select *
        from {{ source("SIGMA", "sigma_new_chains") }}
        where chain_id is not null
    ) as sigma
    on existing.artemis_id = sigma.chain_id
