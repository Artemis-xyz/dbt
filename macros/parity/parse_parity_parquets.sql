{% macro parse_parity_parquets(chain, metric_type) %}        
    {% if metric_type == "fees" %}
        select
            parquet_raw:"chain"::string as chain
            , parquet_raw:"number"::integer as number
            , parquet_raw:"extrinsic_hash"::string as hash
            , parquet_raw:"success"::string as success
            , parquet_raw:"signer_id"::string as signer_id
            , to_timestamp_ntz(parquet_raw:"timestamp"::integer/1000000) as timestamp
            , parquet_raw:"relay_chain"::string as relay_chain
        FROM {{ source("PROD_LANDING", "raw_" ~ chain  ~ "_transactions_parquet") }}
    {% else %}
        select
            parquet_raw:"chain"::string as chain
            , parquet_raw:"number"::integer as number
            , parquet_raw:"extrinsic_hash"::string as hash
            , parquet_raw:"fees"::integer as fees
            , to_timestamp_ntz(parquet_raw:"timestamp"::integer/1000000) as timestamp
            , parquet_raw:"relay_chain"::string as relay_chain
        FROM {{ source("PROD_LANDING", "raw_" ~ chain  ~ "_fees_parquet") }}
    {% endif %}
{% endmacro %}
