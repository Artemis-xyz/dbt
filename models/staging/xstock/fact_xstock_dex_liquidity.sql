{{ config(materialized="table", snowflake_warehouse="ANALYTICS_XL") }}

with
    -- TODO: Remove once we have updated balances data
    old_balances as (
        select
            ab.address
            , ab.contract_address
            , block_timestamp
             -- note Solana balances are already decimals adjusted
            , amount_unadj as balance_raw
            , amount AS balance_native
        from {{ ref("fact_solana_address_balances_by_token") }} ab
        inner join {{ ref("dim_xstock_token_pool_accounts") }} stocks on lower(ab.address) = lower(stocks.token_account)
    )
    , address_balances as (
        select
            ab.address
            , ab.contract_address
            , block_timestamp
            , balance_raw
            , balance_native
        from old_balances ab
        where block_timestamp < to_date(sysdate())
        
    )
    
    , heal_balance_table as (
        -- address_balances and stale_address_balances do not over lap
        -- address_balances select every row greater than the most recent date in the table
        -- stale_address_balances selects the most recent date in the table
        select
            block_timestamp
            , contract_address
            , address
            , balance_raw
            , balance_native
        from address_balances
        
    ) 
    , balances as (
        select 
            block_timestamp::date as date
            , contract_address
            , address
            , balance_raw
            , balance_native
        from (
            select 
                block_timestamp
                , contract_address
                , address
                , balance_raw
                , balance_native
                , row_number() over (partition by block_timestamp::date, contract_address, address order by block_timestamp desc) AS rn
            from heal_balance_table
        )
        where rn = 1
    )
    , date_range as (
        select 
            min(block_timestamp)::date as date
            , contract_address
            , address
        from heal_balance_table
        group by contract_address, address
        
        union all   
        
        select
            dateadd(day, 1, date) as date
            , contract_address
            , address
        from date_range
        where date < dateadd(day, -1, to_date(sysdate()))
    )
    , historical_supply_by_address_balances as (
        select
            date
            , address
            , contract_address
            , coalesce(
                balance_raw, 
                LAST_VALUE(balances.balance_raw ignore nulls) over (
                    partition by contract_address, address
                    order by date
                    rows between unbounded preceding and current row
                ) 
            )  as balance_raw
            , coalesce(
                balance_native, 
                LAST_VALUE(balances.balance_native ignore nulls) over (
                    partition by contract_address, address
                    order by date
                    rows between unbounded preceding and current row
                ) 
            )  as balance_native
        from date_range
        left join balances using (date, contract_address, address)
    )
    , prices as (
        select hour::date as date, token_address as contract_address, symbol, avg(price) as price 
        from solana_flipside.price.ez_prices_hourly p
        where token_address in (select address from {{ ref("xstock_tokens") }})
        group by 1, 2, 3
    )
    , address_balances_with_prices as (
        select
            date
            , contract_address
            , symbol
            , address
            , price
            , balance_raw
            , balance_native
            , balance_native * price as balance
            , 'solana' as chain
            , date || '-' || address || '-' || contract_address as unique_id
        from historical_supply_by_address_balances
        left join prices using (date, contract_address)
    )
select 
    date
    , contract_address
    , symbol
    , address
    , application_id
    , program_id
    , pool_account
    , balance_raw
    , balance_native
    , price
    , balance
    , chain
    , unique_id
from address_balances_with_prices
left join {{ ref("dim_xstock_token_pool_accounts") }} pools 
    on lower(address_balances_with_prices.address) = lower(pools.token_account)

