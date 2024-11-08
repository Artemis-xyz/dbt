{{
    config({
        "materialized": "table"
    })
}}

with max_extraction as (
    SELECT
        max(extraction_date) as max_extraction
    FROM
        {{ source("PROD_LANDING", "raw_dydx_v4_perpetuals_markets_metadata") }}
),
latest_data as (
    SELECT
        parse_json(source_json) as data
    FROM
        {{ source("PROD_LANDING", "raw_dydx_v4_perpetuals_markets_metadata") }}
    WHERE
        extraction_date = (
            select
                max_extraction
            from
                max_extraction
        )
)
select
    value:ticker::string as ticker
    , value:clobPairId::number as clob_pair_id
    , value:atomicResolution::number as atomic_resolution
from
    latest_data
    , lateral flatten (input => data:markets)