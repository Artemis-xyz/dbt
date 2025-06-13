-- depends_on {{ ref("ez_celo_metrics_by_subcategory") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="bam_transaction_sm",
        database="celo",
        schema="core",
        alias="ez_metrics_by_category_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_category_v2("celo") }}
