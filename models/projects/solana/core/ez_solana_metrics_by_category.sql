{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="SOLANA_XLG",
        database="solana",
        schema="core",
        alias="ez_metrics_by_category",
    )
}}

with
    min_date as (
        select
            min(raw_date) as start_date,
            value as signer,
            case when category = 'Tokens' then 'Token' else category end as category
        from {{ ref("ez_solana_transactions") }}, lateral flatten(input => signers)
        where succeeded = 'TRUE'
        group by category, signer
    ),
    new_users as (
        select count(distinct signer) as new_users, category, start_date
        from min_date
        group by start_date, category
    ),
    bot as (
        select
            raw_date,
            category,
            count(distinct signers[0]) as low_sleep_users,
            count(*) as tx_n
        from {{ ref("ez_solana_transactions") }}
        where user_type = 'LOW_SLEEP'
        group by user_type, raw_date, category
    ),
    sybil as (
        select
            raw_date,
            category,
            count(distinct signers[0]) as sybil_users,
            count(*) as tx_n
        from {{ ref("ez_solana_transactions") }}
        where engagement_type = 'sybil'
        group by engagement_type, raw_date, category
    ),
    agg_data as (
        select
            raw_date,
            max(chain) as chain,
            case
                when category = 'Tokens' then 'Token' else category
            end as updated_category,
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then gas_usd else 0 end) gas_usd,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau
        from {{ ref("ez_solana_transactions") }}, lateral flatten(input => signers)
        group by raw_date, updated_category
    )
select
    agg_data.raw_date as date,
    ifnull(agg_data.updated_category, 'Unlabeled') as category,
    agg_data.chain,
    gas,
    gas_usd,
    txns,
    dau,
    (dau - new_users) as returning_users,
    new_users,
    low_sleep_users,
    (dau - low_sleep_users) as high_sleep_users,
    sybil_users,
    (dau - sybil_users) as non_sybil_users
from agg_data
left join
    new_users
    on equal_null(agg_data.updated_category, new_users.category)
    and agg_data.raw_date = new_users.start_date
left join
    bot
    on equal_null(agg_data.updated_category, bot.category)
    and agg_data.raw_date = bot.raw_date
left join
    sybil
    on equal_null(agg_data.updated_category, sybil.category)
    and agg_data.raw_date = sybil.raw_date
