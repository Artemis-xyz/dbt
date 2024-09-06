{% macro convert_routescan_api_fees_native_for_chain(chain) %}
    with
        max_extraction_gas_used as (
            select max(extraction_date) as max_date
            from {{ source("PROD_LANDING", "raw_" ~ chain ~ "_gas_used") }}
            
        ),
        latest_data_gas_used as (
            select parse_json(source_json) as data
            from {{ source("PROD_LANDING", "raw_" ~ chain ~ "_gas_used") }}
            where extraction_date = (select max_date from max_extraction_gas_used)
        ),
        gas_used as (
            select
                f.value:date::date as date,
                f.value:"gas-used"::int as gas_used
            from latest_data_gas_used, lateral flatten(input => data) f
        ),
        max_extraction_avg_gas_price as (
            select max(extraction_date) as max_date
            from {{ source("PROD_LANDING", "raw_" ~ chain ~ "_avg_gas_price") }}
            
        ),
        latest_data_avg_gas_price as (
            select parse_json(source_json) as data
            from {{ source("PROD_LANDING", "raw_" ~ chain ~ "_avg_gas_price") }}
            where extraction_date = (select max_date from max_extraction_avg_gas_price)
        ),
        avg_gas_price as (
            select
                f.value:date::date as date,
                f.value:"avg-gas-price"::int as avg_gas_price
            from latest_data_avg_gas_price, lateral flatten(input => data) f
        )
        select
            gas_used.date,
            gas_used.gas_used,
            avg_gas_price.avg_gas_price,
            gas_used.gas_used * avg_gas_price.avg_gas_price / pow(10, 18) as fees_native
        from gas_used
        left join avg_gas_price on gas_used.date = avg_gas_price.date
        
{% endmacro %}