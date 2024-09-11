{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
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
        , source_json:"from_type"::string as from_type
        , source_json:"to_address"::string as to_address
        , source_json:"to_type"::string as to_type
        , source_json:"url"::string as url
        , source_json:"decimals"::int as decimal
        , source_json:"symbol"::string as symbol
        , source_json:"trace_id"::string as trace_id
        , source_json:"dest_verified"::boolean as dest_verified
        , source_json:"source_verified"::boolean as source_verified
        , source_json:"account_verified"::boolean as account_verified
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
    , max_by(from_type, extraction_date) as from_type
    , max_by(to_address, extraction_date) as to_address
    , max_by(to_type, extraction_date) as to_type
    , max_by(trace_id, extraction_date) as trace_id
    , max_by(decimal, extraction_date) as decimal
    , case when (max_by(symbol, extraction_date) = 'USD₮' or max_by(url, extraction_date) = 'https://tether.to/usdt-ton.json')  then 'USDT' else max_by(symbol, extraction_date) end as symbol
    , max_by(dest_verified, extraction_date) as dest_verified
    , max_by(source_verified, extraction_date) as source_verified
    , max_by(account_verified, extraction_date) as account_verified
from raw_data
where 
    from_address is not null and 
    to_address is not null and 
    timestamp is not null and
    -- Black list transaction: https://tonviewer.com/transaction/513bacb858a3148367643ec9da96ba9e8af8bdf02c1950b106e9a88fe3e94935
    tx_hash not in ('513bacb858a3148367643ec9da96ba9e8af8bdf02c1950b106e9a88fe3e94935')
group by tx_hash
