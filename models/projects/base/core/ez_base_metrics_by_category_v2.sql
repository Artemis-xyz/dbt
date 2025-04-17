-- depends_on {{ ref("ez_base_metrics_by_subcategory") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="base_md",
        database="base",
        schema="core",
        alias="ez_metrics_by_category_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_category_v2("base") }}
