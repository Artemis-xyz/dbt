{{ config(materialized="table", snowflake_warehouse="ANALYTICS_XL") }}
with
token_acccounts as (
    select distinct address
    from {{ref("fact_xstock_balances")}}
)
, xstock_pool_accounts as (
    select owner as pool_account, account_address as token_account
    from solana_flipside.core.fact_token_account_owners
    inner join token_acccounts on address = account_address
)
-- We need to get the token accounts that hold the other side of the pool
, pool_accounts as (
    select distinct owner as pool_account, account_address as token_account
    from solana_flipside.core.fact_token_account_owners
    inner join xstock_pool_accounts on pool_account = owner
)
, dex_program_accounts as (
    select owner as program_id, account_address as pool_account, 
    from solana_flipside.core.fact_token_account_owners
    where owner in (
        'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
        'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
    )
)
select 
    program_id
    , pool_account
    , token_account
    , case 
        when program_id = 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK' then 'raydium'
        when program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' then 'orca'
    end as application_id
from dex_program_accounts
inner join pool_accounts using(pool_account)
