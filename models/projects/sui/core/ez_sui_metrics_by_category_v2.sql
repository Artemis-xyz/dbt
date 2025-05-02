{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
        database="sui",
        schema="core",
        alias="ez_metrics_by_category_v2",
    )
}}
select 
    TO_TIMESTAMP_NTZ(date) as date,
    category,
    max(chain) as chain,
    ROUND(sum(gas), 3) as gas,
    sum(gas_usd) as gas_usd,
    sum(txns) as txns,
    sum(dau) as dau,
    sum(returning_users) as returning_users,
    sum(new_users) as new_users,
    sum(low_sleep_users) as low_sleep_users,
    sum(high_sleep_users) as high_sleep_users,
    sum(sybil_users) as sybil_users,
    sum(non_sybil_users) as non_sybil_users
from {{ ref("ez_sui_metrics_by_subcategory") }}
group by category, date