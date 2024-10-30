{{ config(materialized="table") }}
with
    prices as ( {{ get_coingecko_price_with_latest("ethereum") }})
    , fundamentals as (
        select 
            block_date as date
            , count(distinct case when success = 'TRUE' then hash_hex end) as txns
            , count(distinct from_hex) as daa
            , sum(gas_used * gas_price) / 1E18 as gas
            , median(gas_used * gas_price) / 1E18 as median_gas
        from zksync_dune.zksync.transactions
        group by 1
    )
select
    date
    , 'zksync' as chain
    , txns
    , daa
    , gas
    , gas * price as gas_usd
    , median_gas * price as median_gas_usd
from fundamentals
left join prices using(date)
order by date desc
