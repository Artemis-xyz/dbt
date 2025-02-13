-- depends_on {{ ref("ez_polygon_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="polygon",
        database="polygon",
        schema="core",
        alias="ez_metrics_by_contract_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("polygon", "v2") }}
