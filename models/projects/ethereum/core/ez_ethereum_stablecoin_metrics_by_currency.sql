-- depends_on: {{ ref('fact_ethereum_stablecoin_contracts') }}
{{
    config(
        materialized="table",
        database="ethereum",
        schema="core",
        alias="ez_stablecoin_metrics_by_currency",
        snowflake_warehouse="ETHEREUM_LG",
    )
}}

{{ agg_chain_stablecoin_metrics("ethereum") }}
