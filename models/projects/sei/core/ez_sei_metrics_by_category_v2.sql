-- depends_on {{ ref("ez_sei_metrics_by_subcategory") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="sei_lg",
        database="sei",
        schema="core",
        alias="ez_metrics_by_category_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_category_v2("sei") }}
