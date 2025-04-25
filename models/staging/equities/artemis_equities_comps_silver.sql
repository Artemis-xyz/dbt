{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "artemis_equities_comps") }}
    ),
    data as (
        select 
            value:"fiscal_year"::VARCHAR AS fiscal_year,
            value:"fiscal_period"::VARCHAR AS fiscal_period,
            value:"date"::VARCHAR AS date,
            value:"metric"::VARCHAR AS metric,
            value:"metric_friendly_name"::VARCHAR AS metric_friendly_name,
            value:"factor"::VARCHAR AS factor,
            value:"balance"::VARCHAR AS balance,
            value:"unit"::VARCHAR AS unit,
            value:"value"::VARCHAR AS value,
            value:"ticker"::VARCHAR AS ticker,
            value:"company_name"::VARCHAR AS company_name,
            value:"company_stock_exchange"::VARCHAR AS company_stock_exchange,
            value:"company_cik"::VARCHAR AS company_cik,
            extraction_date
        from
            {{ source("PROD_LANDING", "artemis_equities_comps") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select * from data