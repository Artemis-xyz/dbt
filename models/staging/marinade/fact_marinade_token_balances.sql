{{ config(
    materialized="incremental",
    snowflake_warehouse="MARINADE"
) }}


select 
    date(block_timestamp) as date,
    1000000000 - balance as circulating_supply
from 
    solana_flipside.core.fact_token_balances
where 
    lower(account_address) = lower('GR1LBT4cU89cJWE74CP6BsJTf2kriQ9TX59tbDsfxgSi')
    {% if is_incremental() %}
        and date(block_timestamp) >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% endif %}
order by
    date(block_timestamp) desc