{% macro hyperliquid_token_price(token) %}

with 
    latest_data as (
        select 
            source_json:timestamp::string as timestamp
            , max_by(extraction_date, source_json:timestamp::string) as extraction_date
        from {{ source("PROD_LANDING", "raw_hyperliquid_"~token~"_price_data") }} 
        {% if is_incremental() %}
            where extraction_date >= (select max(block_timestamp) from {{ this }})
        {% endif %}
        group by timestamp
    )
select 
    distinct to_timestamp(TO_CHAR(TO_TIMESTAMP(source_json:timestamp::string / 1000), 'YYYY-MM-DD HH24:MI:SS')) as timestamp
    , source_json:price::float as price
    , source_json:size::float as size
    , source_json:type::string as type
    , source_json:symbol::string as symbol
from {{ source("PROD_LANDING", "raw_hyperliquid_"~token~"_price_data") }}  data
inner join latest_data on latest_data.timestamp = source_json:timestamp::string and latest_data.extraction_date = data.extraction_date

{% endmacro %}