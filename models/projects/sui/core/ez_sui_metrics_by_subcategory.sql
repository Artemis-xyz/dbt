{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
        database="sui",
        schema="core",
        alias="ez_metrics_by_subcategory",
    )
}}
-- Based on the `get_fundamental_data_for_chain_by_category` macro in the primary
-- repo
with
    min_date as (
        select min(raw_date) as start_date, from_address, category, sub_category
        from {{ source("PROD_LANDING", "fact_sui_transactions_silver_bigquery_v2") }}
        group by sub_category, category, from_address
    ),
    new_users as (
        select count(distinct from_address) as new_users, category, sub_category, start_date
        from min_date
        group by start_date, category, sub_category
    ),
    agg_data as (
        select
            raw_date,
            category,
            sub_category,
            max(chain) as chain,
            sum(tx_fee) gas,
            sum(gas_usd) gas_usd,
            count(*) txns,
            count(distinct from_address) dau
        from {{ source("PROD_LANDING", "fact_sui_transactions_silver_bigquery_v2") }}
        group by raw_date, category, sub_category
    )
select
    TO_TIMESTAMP_NTZ(agg_data.raw_date) as date,
    if(agg_data.category is null, 'Unlabeled', cast(agg_data.category as string)) as category,
    if(agg_data.sub_category is null, 'Unlabeled', cast(agg_data.sub_category as string)) as sub_category,
    chain,
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
    on cast(agg_data.category as string) = cast(new_users.category as string)
    and cast(agg_data.sub_category as string) = cast(new_users.sub_category as string)
    and agg_data.raw_date = new_users.start_date
where agg_data.raw_date < current_date()