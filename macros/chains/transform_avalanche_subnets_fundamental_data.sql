{% macro transform_avalanche_subnets_fundamental_data(chain)  %}

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
            value:"value"::float/POW(10, 9) as gas_used
        from
            {{ source("PROD_LANDING", "raw_" ~ chain ~ "_gas") }},
            lateral flatten(input => parse_json(source_json:"results"))
        where extraction_date = (select max_date from gas_extraction)
    ),
    avg_gas_extraction as(
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_" ~ chain ~ "_avg_gas_price") }}
    ),
    avg_gas_price_data as (
        select
            DATEADD('day', 1, TO_TIMESTAMP(value:"timestamp")::date) as date,
            value:"value"::float as avg_gas_price
        from
            {{ source("PROD_LANDING", "raw_" ~ chain ~ "_avg_gas_price") }},
            lateral flatten(input => parse_json(source_json:"results"))
        where extraction_date = (select max_date from avg_gas_extraction)
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
        , gas_used * avg_gas_price as fees_native
        , '{{ chain }}' as chain
    FROM txns_data txns
        FULL JOIN dau_data dau ON txns.date = dau.date
        FULL JOIN gas_data gas ON txns.date = gas.date
        FULL JOIN avg_gas_price_data avg ON txns.date = avg.date
    where coalesce(txns.date, dau.date, gas.date, avg.date) < to_date(sysdate())
{% endmacro %}
