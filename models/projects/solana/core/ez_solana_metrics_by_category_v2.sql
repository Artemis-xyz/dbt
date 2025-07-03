-- depends_on {{ ref('ez_solana_metrics_by_subcategory') }}
{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="SOLANA_XLG",
        database="solana",
        schema="core",
        alias="ez_metrics_by_category_v2",
    )
}}

with
    min_date as (
        select
            min(raw_date) as start_date,
            value as signer,
            case when category = 'Tokens' then 'Token' else category end as category
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
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
        from {{ ref('fact_solana_transactions_v2') }}
        where user_type = 'LOW_SLEEP'
        group by user_type, raw_date, category
    ),
    sybil as (
        select
            raw_date,
            category,
            count(distinct signers[0]) as sybil_users,
            count(*) as tx_n
        from {{ ref('fact_solana_transactions_v2') }}
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
            sum(case when index = 0 then tx_fee + COALESCE(jito_tips, 0) else 0 end) rev,
            sum(case when index = 0 then gas_usd + COALESCE(jito_tips_usd, 0) else 0 end) rev_usd,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        group by raw_date, updated_category
    ),
    monthly_users as (
        select
            date_trunc('month', raw_date) as month_date,
            case when category = 'Tokens' then 'Token' else category end as updated_category,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) mau
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        group by month_date, updated_category
    )
select
    agg_data.raw_date as date,
    ifnull(agg_data.updated_category, 'Unlabeled') as category,
    agg_data.chain,
    gas,
    CASE 
        WHEN dau = 0 OR dau IS NULL THEN NULL
        ELSE gas / dau
    END AS avg_gas_per_address,
    gas_usd,
    CASE 
        WHEN dau = 0 OR dau IS NULL THEN NULL
        ELSE gas_usd / dau
    END AS avg_gas_usd_per_address,
    rev,
    rev_usd,
    txns,
    dau,
    ifnull(monthly_users.mau, 0) as mau,
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
    and agg_data.raw_date = new_users.start_date
left join
    bot
    on equal_null(agg_data.updated_category, bot.category)
    and agg_data.raw_date = bot.raw_date
left join
    sybil
    on equal_null(agg_data.updated_category, sybil.category)
    and agg_data.raw_date = sybil.raw_date
left join
    monthly_users
    on equal_null(agg_data.updated_category, monthly_users.updated_category)
    and date_trunc('month', agg_data.raw_date) = monthly_users.month_date