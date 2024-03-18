with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_stargate_bridge_volume") }}
    ),
    data as (
        select parse_json(source_json) data
        from {{ source("PROD_LANDING", "raw_stargate_bridge_volume") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    to_date(value:"dayDate") as date,
    value:value::double as bridge_volume,
    'stargate' as app,
    null as chain,
    'Bridge' as category
from data, lateral flatten(input => data)
