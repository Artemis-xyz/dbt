with
max_extraction as (
    select max(extraction_date) as max_date
    from {{ source("PROD_LANDING", "raw_icp_daily_block_count") }}
)
,latest_data as (
    select parse_json(source_json) as data
    from {{ source("PROD_LANDING", "raw_icp_daily_block_count") }}
    where extraction_date = (select max_date from max_extraction)
)
select
    value:timestamp::timestamp_ntz::date as date
    , value:block_number::int as block_count
    , 'internet_computer' as chain
from latest_data, lateral flatten(input => data) as f
