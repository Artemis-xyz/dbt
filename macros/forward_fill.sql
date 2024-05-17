{% macro forward_fill(date_col, value_col, table_name) %}
(
    with results as (
        select 
            {{ date_col }} as date,
            {{ value_col }} as value 
        from {{ table_name }}
    ),
    date_range as (
        select 
            -1 + row_number() over (order by 0) as i, 
            start_date + i as date
        from (
            select 
                min(date)::date as start_date, 
                current_date() as end_date
            from results
        ) 
        join table(generator(rowcount => 10000)) x
        qualify i < 1 + end_date - start_date
    ),
    full_range_data as (
        select
            coalesce(date_range.date, results.date) as date,
            value
        from results
        full join date_range on results.date = date_range.date
    ),
    forward_fill_data as (
        select
            date,
            coalesce(
                value,
                lag(value) ignore nulls over (order by date)
            ) as value
        from full_range_data
    )
    select * from forward_fill_data
)
{% endmacro %}
