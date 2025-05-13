{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
        database="sui",
        schema="core",
        alias="ez_metrics_by_application_v2",
    )
}}

-- Based on the `get_fundamental_data_for_chain_by_application` macro in the primary
-- repo
with
    min_date as (
        select min(raw_date) as start_date, from_address, app
        from {{ source("PROD_LANDING", "fact_sui_transactions_silver_bigquery_v2") }}
        where app is not null
        group by app, from_address
    ),
    new_users as (
        select count(distinct from_address) as new_users, app, start_date
        from min_date
        group by start_date, app
    ),
    agg_data as (
        select
            raw_date,
            app,
            max(chain) as chain,
            max(friendly_name) friendly_name,
            max(category) category,
            max(sub_category) sub_category,
            sum(tx_fee) gas,
            sum(gas_usd) gas_usd,
            count(*) txns,
            count(distinct from_address) as dau
        from {{ source("PROD_LANDING", "fact_sui_transactions_silver_bigquery_v2") }}
        where app is not null
        group by raw_date, app
    )

select
    TO_TIMESTAMP_NTZ(agg_data.raw_date) as date,
    agg_data.app,
    chain,
    friendly_name,
    category,
    sub_category,
    ROUND(gas, 3) AS gas,
    gas_usd,
    txns,
    dau,
    (dau - new_users.new_users) as returning_users,
    new_users.new_users,
    null as low_sleep_users,
    null as high_sleep_users,
    null as sybil_users,
    null as non_sybil_users
from agg_data
left join
    new_users
    on agg_data.app = new_users.app
    and agg_data.raw_date = new_users.start_date
where agg_data.raw_date < current_date()