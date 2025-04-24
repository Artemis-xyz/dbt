{{
    config(
        materialized="table",
        snowflake_warehouse="INJECTIVE",
    )
}}

with defillama_metrics as (
    {{ get_defillama_metrics("injective") }}
)
select * from defillama_metrics

