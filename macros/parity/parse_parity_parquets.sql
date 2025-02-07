{% macro parse_parity_parquets(chain, metric_type) %}        
    {% if metric_type == "transactions" %}
        select
            parquet_raw:"chain"::string as chain
            , parquet_raw:"number"::integer as number
            , parquet_raw:"extrinsic_hash"::string as hash
            , parquet_raw:"success"::string as success
            , parquet_raw:"signer_id"::string as signer_id
            , to_timestamp_ntz(parquet_raw:"timestamp"::integer/1000000) as timestamp
            , parquet_raw:"relay_chain"::string as relay_chain
        FROM {{ source("PROD_LANDING", "raw_" ~ chain  ~ "_transactions_parquet") }}
    {% elif metric_type == "burned" %}
        with decimals as (
            select
                chain
                , unit
                , decimals
            from {{ source("MANUAL_STATIC_TABLES", "polkadot_token_decimals") }}
            where chain = '{{ chain }}'
        )
        select
            f.parquet_raw:"chain"::string as chain
            , f.parquet_raw:"type"::string as type
            , f.parquet_raw:"amount"::integer as amount
            , f.parquet_raw:"pallet"::string as pallet
            , f.parquet_raw:"method"::string as method 
            , to_timestamp(f.parquet_raw:"timestamp"::integer/1000000) as timestamp
            , f.parquet_raw as parquet_raw
            , decimals
            , unit
        FROM {{ source("PROD_LANDING", "raw_" ~ chain  ~ "_burned_parquet") }} as f
        LEFT JOIN decimals on f.parquet_raw:"chain"::string = decimals.chain
    {% else %}
    with decimals as (
            select
                chain
                , unit
                , decimals
            from {{ source("MANUAL_STATIC_TABLES", "polkadot_token_decimals") }}
            where chain = '{{ chain }}'
        )
        select
            f.parquet_raw:"chain"::string as chain
            , f.parquet_raw:"number"::integer as number
            , f.parquet_raw:"extrinsic_hash"::string as hash
            , f.parquet_raw:"fees"::integer as fees
            , to_timestamp_ntz(f.parquet_raw:"timestamp"::integer/1000000) as timestamp
            , f.parquet_raw:"relay_chain"::string as relay_chain
            , decimals
            , unit
        FROM {{ source("PROD_LANDING", "raw_" ~ chain  ~ "_fees_parquet") }} as f
        LEFT JOIN decimals on f.parquet_raw:"chain"::string = decimals.chain
    {% endif %}
{% endmacro %}
