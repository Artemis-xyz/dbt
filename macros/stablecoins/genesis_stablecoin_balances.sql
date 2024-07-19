{% macro genesis_stablecoin_balances(source_table, genesis_timestamp) %}
    
    select
        '{{genesis_timestamp}}'::timestamp as block_timestamp
        , lower(value:contract_address::string) as contract_address
        , lower(value:address::string) as address
        , value:balance::float as balance
    from {{ source("PROD_LANDING", source_table)}},
        lateral flatten(input => parse_json(source_json))
    where extraction_date = (select max(extraction_date) from {{ source("PROD_LANDING", source_table)}})
{% endmacro %}