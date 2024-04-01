-- depends_on: {{ ref('fact_bsc_stablecoin_contracts') }}
{{
    config(
        materialized="table",
        database="bsc",
        schema="core",
        alias="ez_stablecoin_metrics_by_currency",
        snowflake_warehouse="BSC_MD",
    )
}}

{{ agg_chain_stablecoin_metrics("bsc") }}
