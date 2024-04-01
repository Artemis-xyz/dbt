{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_defillama_protocol_fees") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_defillama_protocol_fees") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            data:"defillamaId"::string as defillama_protocol_id,
            to_date(convert_timezone('UTC', value[0]::timestamp)) as date,
            value[1]::float as fees
        from latest_data, lateral flatten(input => data:"totalDataChart")
    )

select *
from flattened_data
