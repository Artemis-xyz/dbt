-- depends_on {{ ref("ez_polygon_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="polygon",
        database="polygon",
        schema="core",
        alias="ez_metrics_by_category",
    )
}}

{{ get_fundamental_data_for_chain_by_category_v2("polygon") }}
