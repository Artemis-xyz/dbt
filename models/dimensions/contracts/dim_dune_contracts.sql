{{ config(materialized="table") }}
select
    source_json:address::string as address,
    max_by(source_json:namespace::string, extraction_date) as namespace,
    max_by(source_json:name::string, extraction_date) as name,
    source_json:chain::string as chain
from {{ source("PROD_LANDING", "raw_dune_contracts") }}
group by address, chain
