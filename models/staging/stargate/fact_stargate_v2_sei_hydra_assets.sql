{{config(materialized="table", snowflake_warehouse='STARGATE_MD')}}

WITH 
mints_and_burns as (
    select
        block_timestamp::date as date
        , contract_address
        , lower(t1.symbol) as symbol
        , sum(
            case 
                when from_address = '0x0000000000000000000000000000000000000000' then raw_amount / power(10, t1.decimals)
                else -raw_amount / power(10, t1.decimals)
            end
        ) as amount_native
    from sei_flipside.core_evm.ez_token_transfers
    inner join {{ref('dim_stargate_v2_sei_assets')}} t1 on lower(token_address) = lower(contract_address)
    where (
            from_address = '0x0000000000000000000000000000000000000000'
            or to_address = '0x0000000000000000000000000000000000000000'
        )
    group by 1, 2, 3
)
, dates AS (
    SELECT
        date
    FROM
        {{ref('dim_date_spine')}}
    WHERE date >= (select min(date) from mints_and_burns) and date < to_date(sysdate())
)
, cumulative_mints_and_burns as (
    select
        d.date
        , t.contract_address
        , t.symbol
        , sum(t.amount_native) over (partition by t.contract_address order by d.date) as amount_native
    from dates d
    left join mints_and_burns t using (date)
)
, usdc_prices as (
    select date as date, 'usdc' as symbol, shifted_token_price_usd as price
    from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'usd-coin'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, 'usdc' as symbol, token_current_price as price
    from pc_dbt_db.prod.fact_coingecko_token_realtime_data
    where token_id = 'usd-coin'
)
, eth_prices as (
    select date as date, 'eth' as symbol, shifted_token_price_usd as price
    from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'ethereum'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, 'eth' as symbol, token_current_price as price
    from pc_dbt_db.prod.fact_coingecko_token_realtime_data
    where token_id = 'ethereum'
)
, usdt_prices as (
    select date as date, 'usdt' as symbol, shifted_token_price_usd as price
    from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'tether'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, 'usdt' as symbol, token_current_price as price
    from pc_dbt_db.prod.fact_coingecko_token_realtime_data
    where token_id = 'tether'
)
, prices as (
    select date, symbol, price
    from usdc_prices
    union all
    select date, symbol, price
    from eth_prices
    union all
    select date, symbol, price
    from usdt_prices
)



select
    date
    , 'sei' as chain
    , contract_address
    , amount_native
    , amount_native * price as amount
    , price
from cumulative_mints_and_burns c
left join prices p using (date, c.symbol)

