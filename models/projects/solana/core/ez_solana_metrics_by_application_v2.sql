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
            sum(case when index = 0 then tx_fee + COALESCE(jito_tips, 0) else 0 end) rev,
            sum(case when index = 0 then gas_usd + COALESCE(jito_tips_usd, 0) else 0 end) rev_usd,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        where not equal_null(category, 'EOA') and app is not null
        group by raw_date, app
    ),
    monthly_users as (
        select
            date_trunc('month', raw_date) as month_date,
            app,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) mau
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        where not equal_null(category, 'EOA') and app is not null
        group by month_date, app
    ),
    -- app_tvl as (
    --     select
    --         date,
    --         app,
    --         sum(balance) as tvl
    --     from {{ ref('fact_solana_address_balances_by_token_with_labels') }}
    --     where not equal_null(category, 'EOA') and app is not null
    --     group by date, app
    -- )
select
    agg_data.raw_date as date,
    agg_data.app,
    chain,
    friendly_name,
    case when category = 'Tokens' then 'Token' else category end as category,
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
    mau,
    null AS contract_count,
    null AS real_users,
    (dau - new_users) as returning_users,
    new_users,
    low_sleep_users,
    (dau - low_sleep_users) as high_sleep_users,
    sybil_users,
    (dau - sybil_users) as non_sybil_users,
    NULL AS tvl
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
left join
    monthly_users
    on equal_null(agg_data.app, monthly_users.app)
    and date_trunc('month', agg_data.raw_date) = monthly_users.month_date
-- left join
--     app_tvl
--     on equal_null(agg_data.app, app_tvl.app)
--     and agg_data.raw_date = app_tvl.date

