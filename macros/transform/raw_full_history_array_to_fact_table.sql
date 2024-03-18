{% macro raw_full_history_array_to_fact_table(
    raw_table_name, date_column_name, value_column_name
) %}
    -- Data has the following structure:
    -- {
    -- "extraction_date": "2021-09-01T00:00:00.000Z",
    -- "source_json": [
    -- {
    -- "date_time": "2021-09-01T00:00:00.000Z",
    -- "DAU": 0
    -- },
    -- {
    -- "date_time": "2021-09-02T00:00:00.000Z",
    -- "DAU": 0
    -- },
    -- {
    -- "date_time": "2021-09-03T00:00:00.000Z",
    -- "DAU": 0
    -- },
    -- .... REST OF ROWS 
    -- ]
    -- }
    -- Any arbitrary row will contain the full history of the data, so we can just
    -- extract the latest row by extraction_date
    with
        max_extraction as (
            select max(extraction_date) as max_date from {{ raw_table_name }}
        )
    select
        value:"{{date_column_name}}"::timestamp as date,
        value:"{{value_column_name}}" as {{ value_column_name }}
    from {{ raw_table_name }}, lateral flatten(input => parse_json(source_json))
    where extraction_date = (select max_date from max_extraction)
{% endmacro %}
