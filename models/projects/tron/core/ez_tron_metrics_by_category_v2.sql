-- depends_on: {{ ref("ez_tron_metrics_by_subcategory") }}
{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="TRON_LG",
        database="tron",
        schema="core",
        alias="ez_metrics_by_category_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_category_v2("tron") }}
