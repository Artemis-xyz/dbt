-- depends_on {{ ref('fact_solana_transactions_v2') }}
{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="SOLANA_XLG",
        database="solana",
        schema="core",
        alias="ez_metrics_by_subcategory",
    )
}}

with
    min_date as (
        select
            min(raw_date) as start_date,
            value as signer,
            case when category = 'Tokens' then 'Token' else category end as category,
            sub_category
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        where succeeded = 'TRUE'
        group by category, sub_category, signer
    ),
    new_users as (
        select count(distinct signer) as new_users, category, sub_category, start_date
        from min_date
        group by start_date, category, sub_category
    ),
    bot as (
        select
            raw_date,
            category,
            sub_category,
            count(distinct signers[0]) as low_sleep_users,
            count(*) as tx_n
        from {{ ref('fact_solana_transactions_v2') }}
        where user_type = 'LOW_SLEEP'
        group by user_type, raw_date, category, sub_category
    ),
    sybil as (
        select
            raw_date,
            category,
            sub_category,
            count(distinct signers[0]) as sybil_users,
            count(*) as tx_n
        from {{ ref('fact_solana_transactions_v2') }}
        where engagement_type = 'sybil'
        group by engagement_type, raw_date, category, sub_category
    ),
    agg_data as (
        select
            raw_date,
            max(chain) as chain,
            case
                when category = 'Tokens' then 'Token' else category
            end as updated_category,
            sub_category,
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then gas_usd else 0 end) gas_usd,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        group by raw_date, updated_category, sub_category
    )
select
    agg_data.raw_date as date,
    ifnull(agg_data.updated_category, 'Unlabeled') as category,
    ifnull(agg_data.sub_category, 'Unlabeled') as sub_category,
    agg_data.chain,
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
    on equal_null(agg_data.updated_category, new_users.category)
    and equal_null(agg_data.sub_category, new_users.sub_category)
    and agg_data.raw_date = new_users.start_date
left join
    bot
    on equal_null(agg_data.updated_category, bot.category)
    and equal_null(agg_data.sub_category, bot.sub_category)
    and agg_data.raw_date = bot.raw_date
left join
    sybil
    on equal_null(agg_data.updated_category, sybil.category)
    and equal_null(agg_data.sub_category, sybil.sub_category)
    and agg_data.raw_date = sybil.raw_date
