with
    pathfinder as (
        select
            source_json:address::string as address,
            source_json:name::string as name,
            source_json:chain::string as chain,
            extraction_date
        from {{ source("PROD_LANDING", "raw_chain_scanner_contracts") }} as pathfinder
    ),
    postgres as (
        select address, postgres.name, namespace_link_id, chain.artemis_id as chain
        from {{ source("POSTGRES_REPLICATED", "core_scannercontracts") }} as postgres
        left join
            {{ source("POSTGRES_REPLICATED", "core_asset") }} as chain
            on postgres.asset_link_id = chain.id
    ),
    latest_pathfinder_extraction as (
        select address, max_by(name, extraction_date) as name, chain
        from pathfinder
        group by address, chain
    )
select
    coalesce(pathfinder.address, postgres.address) as address,
    coalesce(pathfinder.name, postgres.name) as name,
    namespace_link_id,
    coalesce(pathfinder.chain, postgres.chain) as chain
from latest_pathfinder_extraction as pathfinder
full join
    postgres
    on pathfinder.address = postgres.address
    and pathfinder.chain = postgres.chain
where pathfinder.name is not null or postgres.name is not null
