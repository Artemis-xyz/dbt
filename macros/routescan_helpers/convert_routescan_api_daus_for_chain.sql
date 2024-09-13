{% macro convert_routescan_api_daus_for_chain(chain) %}
    with
        max_extraction as (
            select max(extraction_date) as max_date
            from {{ source("PROD_LANDING", "raw_" ~ chain ~ "_dau") }}
        ),
        latest_data as (
            select parse_json(source_json) as data
            from {{ source("PROD_LANDING", "raw_" ~ chain ~ "_dau") }}
            where extraction_date = (select max_date from max_extraction)
        ),
        data as (
            select
                f.value:date::date as date,
                f.value:addresses::int as dau
            from latest_data, lateral flatten(input => data) f
        )
    select date, dau
    from data
    where date < to_date(sysdate())
    order by date desc
{% endmacro %}