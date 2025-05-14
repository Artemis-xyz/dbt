{{ config(materialized="incremental", unique_key=["ticker", "fiscal_year", "fiscal_period", "metric"]) }}
with
    {% if is_incremental() %}
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "artemis_equities_comps") }}
    ),
    {% endif %}
    data as (
        select 
            source_json:"fiscal_year"::VARCHAR AS fiscal_year,
            source_json:"fiscal_period"::VARCHAR AS fiscal_period,
            source_json:"date"::VARCHAR AS date,
            source_json:"metric"::VARCHAR AS metric,
            source_json:"metric_friendly_name"::VARCHAR AS metric_friendly_name,
            source_json:"factor"::VARCHAR AS factor,
            source_json:"balance"::VARCHAR AS balance,
            source_json:"unit"::VARCHAR AS unit,
            source_json:"value"::VARCHAR AS value,
            source_json:"ticker"::VARCHAR AS ticker,
            source_json:"company_name"::VARCHAR AS company_name,
            source_json:"company_stock_exchange"::VARCHAR AS company_stock_exchange,
            source_json:"company_cik"::VARCHAR AS company_cik,
            extraction_date
        from
            {{ source("PROD_LANDING", "artemis_equities_comps") }}
        {% if is_incremental() %}
        where extraction_date = (select max_date from max_extraction)
        {% endif %}
    )
select * from data
qualify row_number() over (
    partition by ticker, fiscal_year, fiscal_period, metric 
    order by extraction_date desc
) = 1