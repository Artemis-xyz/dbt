with
max_extraction as (
    select max(extraction_date) as max_date
    from {{ source("PROD_LANDING", "raw_icp_neuron_fund_size") }}
)
,latest_data as (
    select parse_json(source_json) as data
    from {{ source("PROD_LANDING", "raw_icp_neuron_fund_size") }}
    where extraction_date = (select max_date from max_extraction)
)
select
    to_date(convert_timezone('UTC', value:timestamp)) as date
    , value:community_fund_total_staked::int / 10e8 as neuron_funds_staked_native
    , 'internet_computer' as chain
from latest_data, lateral flatten(input => data) as f
