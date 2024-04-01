{{ config(materialized="table", snowflake_warehouse="BAM_TRANSACTION_XLG") }}

with
    min_date as (
        select min(raw_date) as start_date, value as signer, app
        from
            {{ ref("fact_solana_transactions_gold") }},
            lateral flatten(input => signers)
        where not equal_null(category, 'EOA') and app is not null and succeeded = 'TRUE'
        group by app, signer
    ),
    new_users as (
        select count(distinct signer) as new_users, app, start_date
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
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then gas_usd else 0 end) gas_usd,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) daa
        from
            {{ ref("fact_solana_transactions_gold") }},
            lateral flatten(input => signers)
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
    daa,
    (daa - new_users) as returning_users,
    new_users,
    null as low_sleep_users,
    null as high_sleep_users
from agg_data
left join
    new_users
    on equal_null(agg_data.app, new_users.app)
    and agg_data.raw_date = new_users.start_date
