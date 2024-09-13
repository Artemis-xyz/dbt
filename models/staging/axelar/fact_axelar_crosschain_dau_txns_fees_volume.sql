with
max_extraction as (
    select max(extraction_date) as max_date
    from {{ source("PROD_LANDING", "raw_axelar_dau_txns_volume_fees") }}
)
,latest_data as (
    select 
        date(value:timestamp) as date
        , value:users::number as dau
        , value:fee::number as fees
        , value:num_txs::number as txns
        , value:volume::number as volume
    from {{ source("PROD_LANDING", "raw_axelar_dau_txns_volume_fees") }},
    lateral flatten(input => parse_json(source_json):data)
    where extraction_date = (select max_date from max_extraction)
    and value:timestamp <> 0
)
select
    date
    , dau
    , fees
    , txns
    , volume
    , 'axelar' as chain
    , 'axelar' as app
from latest_data
