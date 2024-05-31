with
max_extraction as (
    select max(extraction_date) as max_date
    from {{ source("PROD_LANDING", "raw_icp_canister_total_state") }}
)
,latest_data as (
    select parse_json(source_json) as data
    from {{ source("PROD_LANDING", "raw_icp_canister_total_state") }}
    where extraction_date = (select max_date from max_extraction)
),
parsed_data as (
    select
        value:timestamp::timestamp_ntz as timestamp_ntz
        , value:memory_usage::int / 1.1e+12 as total_canister_state_tib
        , 'internet_computer' as chain
    from latest_data, lateral flatten(input => data) as f
)
select 
    to_date(convert_timezone('UTC', timestamp_ntz)) as date
    , max_by(total_canister_state_tib, timestamp_ntz) as total_canister_state_tib
from parsed_data
group by 1
