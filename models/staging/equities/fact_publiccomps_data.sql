{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_publiccomps_data") }}
    ),
    data as (
        select 
            PARSE_JSON(source_json):"company_name"::VARCHAR AS company_name,
            PARSE_JSON(source_json):"file_name"::VARCHAR AS file_name,
            PARSE_JSON(source_json):"fiscal_quarter"::VARCHAR AS fiscal_quarter,
            PARSE_JSON(source_json):"fiscal_year"::NUMBER AS fiscal_year,
            PARSE_JSON(source_json):"metric"::VARCHAR AS metric,
            PARSE_JSON(source_json):"metric_full_name"::VARCHAR AS metric_full_name,
            PARSE_JSON(source_json):"period"::VARCHAR AS period,
            to_date(to_timestamp(PARSE_JSON(source_json):"period_end"::NUMBER / 1000000)) AS period_end,
            PARSE_JSON(source_json):"ticker"::VARCHAR AS ticker,
            PARSE_JSON(source_json):"url"::VARCHAR AS url,
            PARSE_JSON(source_json):"value"::NUMBER AS value,
            extraction_date
        from {{ source("PROD_LANDING", "raw_publiccomps_data") }}
        where extraction_date = (select max_date from max_extraction)
    )
select 
    concat(
        coalesce(cast(ticker as string), '_this_is_null_'),
        '|',
        coalesce(cast(fiscal_year as string), '_this_is_null_'),
        '|',
        coalesce(cast(fiscal_quarter as string), '_this_is_null_'),
        '|',
        coalesce(cast(metric as string), '_this_is_null_')
    ) as unique_id,
    * 
from data
qualify row_number() over (partition by ticker, fiscal_year, fiscal_quarter, metric order by extraction_date desc) = 1
