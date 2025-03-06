-- depends_on {{ ref("ez_ethereum_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM_LG",
        database="ethereum",
        schema="core",
        alias="ez_metrics_by_application_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_application("ethereum", "v2") }}
