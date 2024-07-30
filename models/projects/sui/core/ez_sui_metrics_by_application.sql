{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="SUI",
        database="sui",
        schema="core",
        alias="ez_metrics_by_application",
    )
}}
with
    min_date as (
        select min(raw_date) as start_date, sender, app
        from {{ ref("ez_sui_transactions") }}
        where not equal_null(category, 'EOA') and app is not null
        group by app, sender
    ),
    new_users as (
        select count(distinct sender) as new_users, app, start_date
        from min_date
        group by start_date, app
    ),
    dau_txns as (
        select
            raw_date,
            app,
            count(*) txns,
            count(distinct sender) dau
        from {{ ref("ez_sui_transactions") }}
        where not equal_null(category, 'EOA') and app is not null and status = 'success'
        group by raw_date, app
    ),
    agg_data as (
        select
            raw_date,
            app,
            max(chain) as chain,
            max(friendly_name) friendly_name,
            max(category) category,
            sum(tx_fee) gas,
            sum(gas_usd) gas_usd
        from {{ ref("ez_sui_transactions") }}
        where not equal_null(category, 'EOA') and app is not null
        group by raw_date, app
    )
select
    agg_data.raw_date as date,
    agg_data.app,
    chain,
    friendly_name,
    category,
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
    dau_txns
    on equal_null(agg_data.app, dau_txns.app)
    and agg_data.raw_date = dau_txns.raw_date
left join
    new_users
    on equal_null(agg_data.app, new_users.app)
    and agg_data.raw_date = new_users.start_date
