-- depends_on {{ ref("ez_ethereum_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM",
        database="ethereum",
        schema="core",
        alias="ez_metrics_by_category",
    )
}}

{{ get_fundamental_data_for_chain_by_category("ethereum") }}
