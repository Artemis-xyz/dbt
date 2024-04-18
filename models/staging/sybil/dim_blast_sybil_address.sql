{{ config(materialized="table", snowflake_warehouse="BLAST") }} 
{{ detect_sybil("blast") }}
