{{config(materialized='view')}}

select 
    parquet_raw:block_number::integer as block_number
    , '2015-07-29'::timestamp as block_timestamp
    , parquet_raw:from_address::string as from_address
    , parquet_raw:to_address::string as to_address
    , parquet_raw:value::integer as value_raw
    , parquet_raw:gas_limit::integer as gas_limit
    , parquet_raw:gas_price::integer as gas_price
    , parquet_raw:gas_used::integer as gas_used
    , parquet_raw:nonce::integer as nonce
    , row_number() over (order by parquet_raw:to_address) as transaction_index
    , parquet_raw:tx_hash::string as tx_hash
from {{ source('PROD_LANDING', 'raw_ethereum_genesis_transactions_parquet') }}
