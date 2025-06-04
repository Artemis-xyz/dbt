{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="SOLANA_XLG",
        database="solana",
        schema="core",
        alias="ez_metrics_by_application_v2",
    )
}}
with
    min_date as (
        select min(raw_date) as start_date, value as signer, app
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        where not equal_null(category, 'EOA') and app is not null and succeeded = 'TRUE'
        group by app, signer
    ),
    new_users as (
        select count(distinct signer) as new_users, app, start_date
        from min_date
        group by start_date, app
    ),
    bot as (
        select
            raw_date,
            app,
            count(distinct signers[0]) as low_sleep_users,
            count(*) as tx_n
        from {{ ref('fact_solana_transactions_v2') }}
        where user_type = 'LOW_SLEEP' and app is not null
        group by user_type, raw_date, app
    ),
    sybil as (
        select
            raw_date,
            app,
            count(distinct signers[0]) as sybil_users,
            count(*) as tx_n
        from {{ ref('fact_solana_transactions_v2') }}
        where engagement_type = 'sybil'
        group by engagement_type, raw_date, app
    ),
    agg_data as (
        select
            raw_date,
            app,
            max(chain) as chain,
            max(friendly_name) friendly_name,
            max(category) category,
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then gas_usd else 0 end) gas_usd,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        where not equal_null(category, 'EOA') and app is not null
        group by raw_date, app
    )
select
    agg_data.raw_date as date,
    agg_data.app,
    chain,
    friendly_name,
    case when category = 'Tokens' then 'Token' else category end as category,
    gas,
    gas_usd,
    txns,
    dau,
    null AS contract_count,
    null AS real_users,
    (dau - new_users) as returning_users,
    new_users,
    low_sleep_users,
    (dau - low_sleep_users) as high_sleep_users,
    sybil_users,
    (dau - sybil_users) as non_sybil_users
from agg_data
left join
    new_users
    on equal_null(agg_data.app, new_users.app)
    and agg_data.raw_date = new_users.start_date
left join
    bot on equal_null(agg_data.app, bot.app) and agg_data.raw_date = bot.raw_date
left join
    sybil
    on equal_null(agg_data.app, sybil.app)
    and agg_data.raw_date = sybil.raw_date
