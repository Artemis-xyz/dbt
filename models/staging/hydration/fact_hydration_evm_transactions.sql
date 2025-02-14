select 
    parquet_raw:"chain"::string as chain
    , parquet_raw:"data" as data
    , parquet_raw:"ev_method"::string as ev_method
    , parquet_raw:"ev_pallet"::string as ev_pallet
    , parquet_raw:"evm_txn_hash"::string as evm_txn_hash
    , parquet_raw:"ex_hash"::string as evm_hash
    , parquet_raw:"number"::string as number
    , parquet_raw:"receiver"::string as receiver 
    , parquet_raw:"relay_chain"::string as relay_chain
    , parquet_raw:"signer"::string as signer 
    , to_timestamp(parquet_raw:"timestamp"::integer/1000000) as timestamp
    , parquet_raw:"timestamp" as timestamp_raw
from {{ source("PROD_LANDING", "raw_hydration_evm_transactions_parquet") }} 
