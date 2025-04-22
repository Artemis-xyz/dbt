{{ config(snowflake_warehouse="EULER", materialized="table") }}
{{ euler_borrow_and_lending_metrics("sonic") }}