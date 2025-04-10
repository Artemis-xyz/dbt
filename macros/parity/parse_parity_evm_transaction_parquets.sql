{% macro parse_parity_evm_transaction_parquets(chain, coingecko_id) %}
with 
    {% if chain == 'hydration' %}
    decimals as (
        select
            chain
            , unit
            , decimals
        from 
        (
            values
                ('hydration', 'WETH', 18)
            as t (chain, unit, decimals)
        )
    )
    {% else %}
    decimals as (
        select
            chain
            , unit
            , decimals
        from {{ source("MANUAL_STATIC_TABLES", "polkadot_token_decimals") }}
        where chain = '{{chain}}'
    )
    {% endif %}
    , prices as (
        select date as date, shifted_token_price_usd as price
        from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted_gold
        where
            coingecko_id = '{{coingecko_id}}'
            and date < dateadd(day, -1, to_date(sysdate()))
        union
        select dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
        from PC_DBT_DB.PROD.fact_coingecko_token_realtime_data
        where token_id = '{{coingecko_id}}'
    ), evm_transactions as (
        select
            parquet_raw:"chain"::string as chain
            , parquet_raw:"block_number"::number as block_number
            , parquet_raw:"tx_hash"::string as txn_hash
            , parquet_raw:"receiver"::string as receiver 
            , parquet_raw:"sender"::string as sender 
            , parquet_raw:"date"::date as date
            , to_timestamp(parquet_raw:"timestamp"::int/1000) as timestamp
            , parquet_raw:"timestamp" as timestamp_raw
            , parquet_raw:"fee_paid"::number  as fee_paid
            , parquet_raw:"gas_used"::number  as gas_used
            , parquet_raw:"gas_price"::number as gas_price
            , parquet_raw
        from {{ source("PROD_LANDING", 'raw_'~ chain ~ '_evm_transactions_fees_parquet') }} 
    )
    select
            t.chain
            , block_number
            , txn_hash
            , receiver 
            , sender 
            , t.date
            , timestamp
            , timestamp_raw
            , (fee_paid / POW(10, decimals)) * price as fees
            , fee_paid / POW(10, decimals) as fees_native
            , gas_used
            , gas_price
            , parquet_raw
    FROM evm_transactions as t
    left join prices on prices.date = t.date
    left join decimals on t.chain = decimals.chain
    where t.date < date(sysdate())

{% endmacro %}
