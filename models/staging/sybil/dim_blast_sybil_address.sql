{{ config(materialized="table", snowflake_warehouse="BLAST", enabled=false) }} 
{{ detect_sybil("blast") }}
