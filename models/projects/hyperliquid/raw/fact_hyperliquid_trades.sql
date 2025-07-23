{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="HYPERLIQUID",
        database="hyperliquid",
        schema="raw",
        alias="fact_hyperliquid_trades",
        unique_key=["transaction_hash", "trade_id"],
    )
}}
select 
    DATE(TO_TIMESTAMP(parquet_raw:time::BIGINT/1000)) AS date,
    TO_TIMESTAMP(parquet_raw:time::BIGINT/1000) as trade_timestamp,
    parquet_raw:transaction_hash::VARCHAR as transaction_hash,
    parquet_raw:hash::VARCHAR as hash,
    parquet_raw:coin::VARCHAR as coin,
    parquet_raw:feeToken::VARCHAR as fee_token,
    parquet_raw:fee::FLOAT as fee,
    parquet_raw:dir::VARCHAR AS direction,
    parquet_raw:closedPnl::FLOAT as closed_pnl,
    parquet_raw:crossed::BOOLEAN as crossed,
    parquet_raw:tid::BIGINT as trade_id,
    parquet_raw:oid::BIGINT as order_id,
    parquet_raw:cloid::VARCHAR AS client_order_id,
    -- BID vs. ASK
    parquet_raw:side::VARCHAR as side,
    parquet_raw:startPosition::FLOAT as start_position,
    parquet_raw:px::FLOAT as price,
    parquet_raw:sz::FLOAT as size,
    _load_timestamp_utc as _load_timestamp_utc
from {{ source('SNOWPIPE_DB', 'FACT_HYPERLIQUID_TRADES') }}
where
    1=1 
    {% if is_incremental() %}
    AND trade_timestamp > (SELECT MAX(trade_timestamp) FROM {{ this }})
    {% endif %}
