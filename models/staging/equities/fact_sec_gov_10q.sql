{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "equities_metrics_asset") }}
    ),
    data as (
        select 
            value:"cik"::NUMBER AS cik,
            value:"adsh"::NUMBER AS adsh,
            value:"company_name"::VARCHAR AS company_name,
            value:"metric_name"::VARCHAR AS metric_name,
            value:"metric_value"::VARCHAR AS metric_value,
            value:"parent_metric"::VARCHAR AS parent_metric,
            value:"time_period"::VARCHAR AS time_period,
            extraction_date
        from
            {{ source("PROD_LANDING", "equities_metrics_asset") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select * from data