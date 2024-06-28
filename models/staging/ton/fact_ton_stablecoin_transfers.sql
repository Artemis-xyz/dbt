{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="TON",
    )
}}
with raw_data as (
    select
        extraction_date
        , source_json
        , source_json:"amount"::bigint as amount
        , source_json:"date_timestamp"::timestamp as timestamp
        , source_json:"decoded_hash"::string as tx_hash
        , PARSE_JSON(source_json:"decoded_value"::string) as payload
        , source_json:"from_address"::string as from_address
        , source_json:"to_address"::string as to_address
        , source_json:"decimals"::int as decimal
        , source_json:"symbol"::string as symbol
    from
        {{ source("PROD_LANDING", "raw_ton_stablecoin_transfers") }}
    {% if is_incremental() %}
        where source_json:"date_timestamp"::timestamp > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
)
select 
    tx_hash
    , coalesce(max_by(amount, extraction_date), 0) as amount
    , max_by(timestamp, extraction_date) as block_timestamp
    , max_by(payload, extraction_date) as payload
    , max_by(from_address, extraction_date) as from_address
    , max_by(to_address, extraction_date) as to_address
    , max_by(decimal, extraction_date) as decimal
    , max_by(symbol, extraction_date) as symbol
from raw_data
where from_address is not null and to_address is not null and timestamp is not null
group by tx_hash
