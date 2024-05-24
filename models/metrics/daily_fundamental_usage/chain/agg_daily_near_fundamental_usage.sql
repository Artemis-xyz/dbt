-- depends_on {{ ref("ez_near_transactions") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM_MD") }}
{{ get_fundamental_data_for_chain("near") }}
