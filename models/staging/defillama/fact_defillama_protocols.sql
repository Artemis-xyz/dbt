{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_defillama_protocol_data") }}
    ),
    protocol_data as (
        select extraction_date::date as date, parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_defillama_protocol_data") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date,
            value:"id"::string as id,
            value:"name"::string as name,
            value:"symbol"::string as symbol,
            value:"chain"::string as chain,
            value:"logo"::string as logo,
            value:"twitter"::string as twitter,
            value:"url"::string as url,
            value:"gecko_id"::string as gecko_id,
            value:"category"::string as category,
            value:"slug"::string as slug,
            value:"parentProtocol"::string as parent_protocol
        from protocol_data, lateral flatten(input => data)
    )

select *
from flattened_data
