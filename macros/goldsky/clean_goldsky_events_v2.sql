{% macro clean_goldsky_events_v2(chain) %}
with 
data as (
    select
        parquet_raw:block_number::integer as block_number
        , parquet_raw:block_timestamp::timestamp_ntz as block_timestamp
        , parquet_raw:transaction_hash::string as transaction_hash
        , parquet_raw:transaction_index::integer as transaction_index
        , parquet_raw:log_index::integer as event_index
        , coalesce(parquet_raw:contract_address::string, parquet_raw:address::string) as contract_address
        , parquet_raw:data::string as data
        , SPLIT(parquet_raw:topics::string, ',') as topics
        , SPLIT(parquet_raw:topics::string, ',')[0]::string as topic_zero
    from {{ source("PROD_LANDING", "raw_" ~ chain  ~ "_logs_parquet") }}
    {% if is_incremental() %}
        where
            parquet_raw:block_timestamp::timestamp_ntz
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
)
SELECT
    block_number
    , block_timestamp
    , transaction_hash
    , transaction_index
    , event_index
    , contract_address
    , topic_zero
    , topics
    , concat(coalesce(substring(topics[1], 3), ''), coalesce(substring(topics[2], 3), ''), coalesce(substring(topics[3], 3),'')) as topic_data
    , data
FROM data
qualify row_number() over (partition by transaction_hash, event_index order by block_timestamp desc) = 1
{% endmacro %}
