with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_chainlink_daily_req_unique_req_and_fees") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_chainlink_daily_req_unique_req_and_fees") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            value:day::date as date,
            value:"total_reqs"::int as total_requests,
            value:"total_unique_requesters"::int as total_unique_requesters,
            value:"total_usd_fees"::float as total_usd_fees
        from latest_data, lateral flatten(input => data)
    )
select date, total_requests, total_unique_requesters, total_usd_fees
from flattened_data
order by date desc
