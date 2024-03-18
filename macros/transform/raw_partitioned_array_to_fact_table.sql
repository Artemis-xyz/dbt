{% macro raw_partitioned_array_to_fact_table(
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
    -- ]
    -- }
    -- Any arbitrary row will NOT contain the full history of the data, so we need to
    -- extract the data for each date and then break ties by extraction_date
    with
        dates as (
            select
                extraction_date,
                flat_json.value:"{{date_column_name}}"::timestamp as date
            from
                {{ raw_table_name }},
                lateral flatten(input => parse_json(source_json)) as flat_json
            group by date, extraction_date
        ),
        max_extraction_per_day as (
            select date, max(extraction_date) as extraction_date
            from dates
            group by date
            order by date
        ),
        flattened_json as (
            select
                extraction_date,
                flat_json.value:"{{date_column_name}}"::timestamp as date,
                flat_json.value:"{{value_column_name}}" as {{ value_column_name }}
            -- THIS PROBABLY CAN SUPPORT MULTIPLE VALUE COLUMNS
            from
                {{ raw_table_name }},
                lateral flatten(input => parse_json(source_json)) as flat_json
        ),
        map_reduce_json as (
            select t1.*
            from flattened_json t1
            left join max_extraction_per_day t2 on t1.date = t2.date
            where t1.extraction_date = t2.extraction_date
        )
    select *
    from map_reduce_json
    where date < to_date(sysdate())
{% endmacro %}
