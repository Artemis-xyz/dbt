{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_fantom_contracts") }}
    )
select
    date(value:date) as date,
    value:contract_deployers as contract_deployers,
    value:contracts_deployed as contracts_deployed,
    value as source,
    'fantom' as chain
from
    {{ source("PROD_LANDING", "raw_fantom_contracts") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
