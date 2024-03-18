-- depends_on: {{ ref('fact_polygon_stablecoin_contracts') }}
{{
    config(
        materialized="table",
        database="polygon",
        schema="core",
        alias="ez_stablecoin_metrics_by_currency",
        snowflake_warehouse="polygon",
    )
}}

{{ agg_chain_stablecoin_metrics("polygon") }}
