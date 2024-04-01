-- depends_on {{ ref("ez_polygon_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="polygon",
        database="polygon",
        schema="core",
        alias="ez_metrics_by_application",
    )
}}

{{ get_fundamental_data_for_chain_by_application("polygon") }}
