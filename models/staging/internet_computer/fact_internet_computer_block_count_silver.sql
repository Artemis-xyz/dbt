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
, extracted_data as (
    select
        to_date(convert_timezone('UTC', value:timestamp)) as date
        , value:block_number::int as block_count
    from latest_data, lateral flatten(input => data) as f
)
, ranked_data as (
    -- there are multiple rows for the same date, so we need to rank them
    select 
        date,
        coalesce(block_count, 0) as block_count,
        row_number() over (partition by date order by date desc) as row_num
    from extracted_data
)
select
    date
    , block_count
    , 'internet_computer' as chain
from ranked_data
where row_num = 1

