-- depends_on {{ ref("ez_avalanche_metrics_by_subcategory") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE_MD",
        database="avalanche",
        schema="core",
        alias="ez_metrics_by_category_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_category_v2("avalanche") }}
