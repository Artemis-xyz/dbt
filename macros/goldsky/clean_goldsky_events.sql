{% macro clean_goldsky_events(chain) %}
with 
data as (
    select
        parquet_raw:block_number::integer as block_number
        , parquet_raw:block_hash::string as block_hash
        , parquet_raw:block_timestamp::timestamp_ntz as block_timestamp
        , parquet_raw:transaction_hash::string as transaction_hash
        , parquet_raw:transaction_index::integer as transaction_index
        , parquet_raw:log_index::integer as event_index
        , parquet_raw:contract_address::string as contract_address
        , parquet_raw:data::string as data
        , ARRAY_SLICE(SPLIT(parquet_raw:topics::string, ','), 1, ARRAY_SIZE(SPLIT(parquet_raw:topics::string, ','))) as topics
        , SPLIT(parquet_raw:topics::string, ',')[0]::string as topic_0
    from {{ source("PROD_LANDING", "raw_" ~ chain  ~ "_logs_parquet") }}
    {% if is_incremental() %}
    where
        parquet_raw:block_timestamp::timestamp_ntz
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
    ),
cleaned_events as (
    SELECT 
        SUBSTRING(value, 3) as cleaned_topics,
        *
    FROM data, lateral flatten(input => data.topics)
),
collapsed_events as (
    SELECT
        ARRAY_TO_STRING(ARRAY_AGG(cleaned_topics), '')  as topic_data
        , max(block_number) as block_number
        , max(block_hash) as block_hash
        , max(block_timestamp) as block_timestamp
        , transaction_hash
        , max(transaction_index) as transaction_index
        , event_index
        , max(contract_address) as contract_address
        , max(data) as data
        , ARRAY_AGG(value) as topics
        , max(topic_0) as topic_zero
    FROM cleaned_events
    GROUP BY transaction_hash, event_index
)
SELECT
    topic_data
    , data
    , block_number
    , block_hash
    , block_timestamp
    , transaction_hash
    , transaction_index
    , event_index
    , contract_address
    , topics
    , topic_zero
FROM collapsed_events
{% endmacro %}
