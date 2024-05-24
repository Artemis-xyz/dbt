-- depends_on {{ ref("ez_bsc_transactions") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM_LG") }}
{{ get_fundamental_data_for_chain("bsc") }}
