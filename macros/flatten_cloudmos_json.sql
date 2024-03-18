{% macro flatten_cloudmos_json(raw_table_name, column_name) %}
    with
        max_extraction as (
            select max(extraction_date) as max_date
            from landing_database.prod_landing.{{ raw_table_name }}
        ),
        latest_data as (
            select parse_json(source_json) as data
            from landing_database.prod_landing.{{ raw_table_name }}
            where extraction_date = (select max_date from max_extraction)
        ),
        flattened_snapshots as (
            select
                f.value:"date"::string as snapshot_date,
                f.value:"value"::float as snapshot_value
            from latest_data, lateral flatten(input => data:snapshots) f
        )
    select snapshot_date as date, snapshot_value as {{ column_name }}
    from flattened_snapshots
    where date < to_date(sysdate())
    order by date desc
{% endmacro %}
