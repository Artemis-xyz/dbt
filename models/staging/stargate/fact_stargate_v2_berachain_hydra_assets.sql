{{config(materialized="table", snowflake_warehouse='STARGATE_MD')}}

WITH 
mints_and_burns as (
    select
        block_timestamp::date as date
        , contract_address
        , coingecko_id
        , sum(
            case 
                when from_address = '0x0000000000000000000000000000000000000000' then amount_raw / power(10, decimals)
                else -amount_raw / power(10, decimals)
            end
        ) as amount_native
    from {{ref('fact_berachain_token_transfers')}}
    inner join {{ref('dim_stargate_v2_berachain_assets')}} on lower(token_address) = lower(contract_address)
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
        , t.coingecko_id
        , sum(t.amount_native) over (partition by t.contract_address order by d.date) as amount_native
    from dates d
    left join mints_and_burns t using (date)
)
, usdc_prices as (
    select date as date, 'usd-coin' as coingecko_id, shifted_token_price_usd as price
    from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'usd-coin'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, 'usd-coin' as coingecko_id, token_current_price as price
    from pc_dbt_db.prod.fact_coingecko_token_realtime_data
    where token_id = 'usd-coin'
)
, eth_prices as (
    select date as date, 'ethereum' as coingecko_id, shifted_token_price_usd as price
    from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'ethereum'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, 'ethereum' as coingecko_id, token_current_price as price
    from pc_dbt_db.prod.fact_coingecko_token_realtime_data
    where token_id = 'ethereum'
)
, prices as (
    select date, coingecko_id, price
    from usdc_prices
    union all
    select date, coingecko_id, price
    from eth_prices
)


select
    date
    , 'berachain' as chain
    , contract_address
    , amount_native
    , amount_native * price as amount
    , price
from cumulative_mints_and_burns c
left join prices p using (date, c.coingecko_id)

