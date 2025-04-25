{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "artemis_equities_prices") }}
    ),
    data as (
        select 
            value:"ticker"::VARCHAR AS ticker,
            value:"company_name"::VARCHAR AS company_name,
            value:"date"::DATE AS date,
            value:"value"::NUMBER(30, 2) AS value,
            extraction_date
        from
            {{ source("PROD_LANDING", "artemis_equities_prices") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select * from data