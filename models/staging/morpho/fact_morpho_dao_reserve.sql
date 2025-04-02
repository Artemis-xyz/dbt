{{ config(materialized="table", snowflake_warehouse="MEDIUM") }}


with date_spine as (
    select 
        date
    from {{ ref("dim_date_spine") }}
    where date between '2024-11-12' and to_date(sysdate())
)

, morpho_ethereum_dao_reserve as (
    select
        date(block_timestamp) as date
        , user_address
        , contract_address
        , balance / 1e18 as balance
        , 'ethereum' as chain
    from ethereum_flipside.core.fact_token_balances
    where
        lower(contract_address) = lower('0x58D97B57BB95320F9a05dC918Aef65434969c2B2')
        and lower(user_address) = lower('0xcBa28b38103307Ec8dA98377ffF9816C164f9AFa')
        and date(block_timestamp) >= '2024-11-12'
)

-- , morpho_base_dao_reserve as (
--     select
--         date(block_timestamp) as date
--         , lower(address) as user_address
--         , lower(contract_address) as contract_address
--         , balance_token / 1e18 as balance
--         , 'base' as chain
--     from pc_dbt_db.prod.fact_base_address_balances_by_token
--     where
--         address = '0xcba28b38103307ec8da98377fff9816c164f9afa'
--         and contract_address = '0xbaa5cc21fd487b8fcc2f632f3f4e8d37262a0842'
--         and date(block_timestamp) >= '2024-11-12'
-- )

-- , combined_balances AS (
--     select * from morpho_ethereum_dao_reserve
--     union all
--     select * from morpho_base_dao_reserve
-- )

-- , daily_checkpoint AS (
--     select
--         date
--         , chain
--         , user_address
--         , contract_address
--         , balance
--     from (
--         select
--             *,
--             row_number() over (partition by date, chain order by date) as rn
--         from combined_balances
--     )
--     where rn = 1
-- )

-- , date_chain_spine AS (
--     select 
--         ds.date
--         , ch.chain
--     from date_spine ds
--     cross join (select 'ethereum' as chain union all select 'base') ch
-- )

-- , eth_base_join_balances AS (
--     select 
--         dcs.date
--         , dcs.chain
--         , dc.user_address
--         , dc.contract_address
--         , dc.balance
--     from date_chain_spine dcs
--     left join daily_checkpoint dc
--         on dcs.date = dc.date and dcs.chain = dc.chain
-- )

, backfilled_balances AS (
    select
        d.date
        -- , chain
        , last_value(m.user_address ignore nulls) over (
            order by d.date 
            rows between unbounded preceding and current row
        ) as user_address,
        last_value(m.contract_address ignore nulls) over (
            order by d.date 
            rows between unbounded preceding and current row
        ) as contract_address,
        last_value(m.balance ignore nulls) over (
            order by d.date 
            rows between unbounded preceding and current row
        ) as balance
    from date_spine d
    left join morpho_ethereum_dao_reserve m on m.date = d.date
) 

-- , daily_total_balance as (
--     select
--         date
--         , max(user_address) as user_address
--         , max(contract_address) as contract_address
--         , sum(balance) as total_balance
--     from backfilled_balances
--     group by date
--     order by date asc
-- )

, dao_reserve_change as (
    select
        date
        , user_address
        , contract_address
        , balance
        , lead(balance) over (order by date desc) - balance as dao_reserve_change
    from backfilled_balances
)

select
    date
    , user_address
    , contract_address
    , balance
    , dao_reserve_change
from dao_reserve_change
where date >= '2024-11-21'
order by date desc