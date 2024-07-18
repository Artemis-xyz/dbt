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
        , source_json:"dau"::bigint as daa
    from
        {{ source("PROD_LANDING", "raw_ton_app_daa") }}
    {% if is_incremental() %}
        where source_json:"date_timestamp"::timestamp > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
)
select 
    date
    , max_by(daa, extraction_date) as daa
from raw_data
group by date
