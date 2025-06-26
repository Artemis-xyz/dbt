{{ config(snowflake_warehouse=var('snowflake_warehouse', default='EULER'), materialized="table") }}
{{ euler_borrow_and_lending_metrics("base") }}