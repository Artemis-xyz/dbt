{% macro transfrom_avalanche_subnets_fundamental_data(chain)  %}

with
    dau_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_" ~ chain ~ "_dau") }}
    ),
    dau_data as (
        select
            DATEADD('day', 1, TO_TIMESTAMP(value:"timestamp")::date) as date,
            value:"value"::float as dau
        from
            {{ source("PROD_LANDING", "raw_" ~  chain ~ "_dau") }},
            lateral flatten(input => parse_json(source_json:"results"))
        where extraction_date = (select max_date from dau_extraction)
    ),
    gas_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_" ~ chain ~ "_gas") }}
    ),
    gas_data as (
        select
            DATEADD('day', 1, TO_TIMESTAMP(value:"timestamp")::date) as date,
            value:"value"::float/POW(10, 9) as fees_native
        from
            {{ source("PROD_LANDING", "raw_" ~ chain ~ "_gas") }},
            lateral flatten(input => parse_json(source_json:"results"))
        where extraction_date = (select max_date from gas_extraction)
    ),
    txns_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_" ~ chain ~ "_txns") }}
    ),
    txns_data as (
        select
            DATEADD('day', 1, TO_TIMESTAMP(value:"timestamp")::date) as date,
            value:"value"::float as txns
        from
            {{ source("PROD_LANDING", "raw_" ~ chain ~ "_txns") }},
            lateral flatten(input => parse_json(source_json:"results"))
        where extraction_date = (select max_date from txns_extraction)
    )

    SELECT 
        coalesce(txns.date, dau.date, gas.date) as date
        , txns
        , dau
        , fees_native
        , '{{ chain }}' as chain
    FROM txns_data txns
        FULL JOIN dau_data dau ON txns.date = dau.date
        FULL JOIN gas_data gas ON txns.date = gas.date
{% endmacro %}
