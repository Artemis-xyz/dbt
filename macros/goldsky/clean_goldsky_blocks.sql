{% macro clean_goldsky_blocks(chain) %}
with 
data as (
    select
        parquet_raw:block_hash::string as block_hash
        , parquet_raw:block_number::integer as block_number
        , parquet_raw:block_timestamp::timestamp_ntz as block_timestamp
        , coalesce(try_cast(parquet_raw:base_fee_per_gas::string as integer), 0) as base_fee_per_gas
        , coalesce(try_cast(parquet_raw:difficulty::string as integer), 0) as difficulty
        , parquet_raw:extra_data::string as extra_data
        , coalesce(try_cast(parquet_raw:gas_limit::string as integer), 0) as gas_limit
        , coalesce(try_cast(parquet_raw:gas_used::string as integer), 0) as gas_used
        , parquet_raw:id::string as id
        , parquet_raw:logs_bloom::string as logs_bloom
        , parquet_raw:miner::string as miner
        , parquet_raw:nonce::string as nonce
        , parquet_raw:parent_hash::string as parent_hash
        , parquet_raw:receipts_root::string as receipts_root
        , coalesce(try_cast(parquet_raw:size::string as integer), 0) as size
        , parquet_raw:state_root::string as state_root
        , parquet_raw:total_difficulty::string as total_difficulty
        , coalesce(try_cast(parquet_raw:transaction_count::string as integer), 0) as transaction_count
        , parquet_raw:transactions_root::string as transactions_root
        from {{ source("PROD_LANDING", "raw_" ~ chain  ~ "_blocks_parquet") }}
        {% if is_incremental() %}
            where parquet_raw:block_timestamp::timestamp_ntz >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
)
select
    block_hash
    , block_number
    , block_timestamp
    , base_fee_per_gas
    , difficulty
    , extra_data
    , gas_limit
    , gas_used
    , id
    , logs_bloom
    , miner
    , nonce
    , parent_hash
    , receipts_root
    , size
    , state_root
    , total_difficulty
    , transaction_count
    , transactions_root
from data
qualify row_number() over (partition by block_hash order by block_timestamp desc) = 1
{% endmacro %}