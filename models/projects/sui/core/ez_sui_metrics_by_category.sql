{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="SUI",
        database="sui",
        schema="core",
        alias="ez_metrics_by_category",
    )
}}
with
    min_date as (
        select min(raw_date) as start_date, sender, category
        from {{ ref("ez_sui_transactions") }}
        where not equal_null(category, 'EOA')
        group by category, sender
    ),
    new_users as (
        select count(distinct sender) as new_users, category, start_date
        from min_date
        group by start_date, category
    ),
    agg_data as (
        select
            raw_date,
            category,
            max(chain) as chain,
            max(friendly_name) friendly_name,
            sum(tx_fee) gas,
            sum(gas_usd) gas_usd,
            count(*) txns,
            count(distinct sender) dau
        from {{ ref("ez_sui_transactions") }}
        where not equal_null(category, 'EOA')
        group by raw_date, category
    )
select
    agg_data.raw_date as date,
    agg_data.category,
    chain,
    friendly_name,
    gas,
    gas_usd,
    txns,
    dau,
    (dau - new_users) as returning_users,
    new_users,
    null as low_sleep_users,
    null as high_sleep_users,
    null as sybil_users,
    null as non_sybil_users
from agg_data
left join
    new_users
    on equal_null(agg_data.category, new_users.category)
    and agg_data.raw_date = new_users.start_date
