-- depends_on: {{ ref('fact_ethereum_stablecoin_contracts') }}
{{
    config(
        materialized="incremental",
        unique_key=["date", "symbol"],
        database="celo",
        schema="core",
        alias="ez_stablecoin_metrics_by_currency",
        snowflake_warehouse="STABLECOIN_V2_LG_2",
    )
}}

{{stablecoin_metrics_by_currency("celo")}}

