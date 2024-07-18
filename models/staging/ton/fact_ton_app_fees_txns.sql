{{
    config(
        materialized="incremental",
        unique_key="date",
    )
}}
with raw_data as (
    select
        extraction_date
        , source_json
        , source_json:"raw_date"::date as date
        , source_json:"fees_native"::double as fees_native
        , source_json:"txns"::bigint as txns
        , sourec_json:"avg_txn_fee_native"::double as avg_txn_fee_native
    from
        {{ source("PROD_LANDING", "raw_ton_app_txns_fees") }}
    {% if is_incremental() %}
        where source_json:"raw_date"::date > (select dateadd('day', -3, max(date)) from {{ this }})
    {% endif %}
)
select 
    date
    , max_by(fees_native, extraction_date) as fees_native
    , max_by(txns, extraction_date) as txns
    , max_by(avg_txn_fee_native, extraction_date) as avg_txn_fee_native
from raw_data
group by date
