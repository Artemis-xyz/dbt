-- depends_on {{ ref("fact_mantle_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="bam_transaction_sm",
        database="mantle",
        schema="core",
        alias="ez_metrics_by_application_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_application("mantle", "v2") }}
