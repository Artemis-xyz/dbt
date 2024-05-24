-- depends_on {{ ref("ez_ethereum_transactions") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM_MD") }}
{{ get_fundamental_data_for_chain("ethereum") }}
