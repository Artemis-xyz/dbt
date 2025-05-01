{{ config(snowflake_warehouse="EULER", materialized="table", enabled=false) }}
{{ euler_borrow_and_lending_metrics("bob") }}