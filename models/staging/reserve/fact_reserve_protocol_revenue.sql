{{
    config(
        materialized="table",
        snowflake_warehouse="RESERVE",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_reserve_protocol_revenue") }}
        
)
, latest_data as (
    select parse_json(source_json) as data
    from {{ source("PROD_LANDING", "raw_reserve_protocol_revenue") }}
    where extraction_date = (select max_date from max_extraction)
)
, revenue_data as (
    select
        f.value:block_date::date as date
        , f.value:contract_address::string as contract_address
        , f.value:data_value::number as data_value
        , f.value:tx_hash::string as tx_hash
    from latest_data, lateral flatten(input => data) f
)
, address_to_symbol as ( -- manual mapping
    select column1 as contract_address, column2 as symbol
    from (values
        ('0x000000000000000000000000acdf0dba4b9839b96221a8487e9ca660a48212be', 'hyUSD'),
        ('0x000000000000000000000000e72b141df173b999ae7c1adcbf60cc9833ce56a8', 'ETH+'),
        ('0x000000000000000000000000a0d69e286b938e21cbf7e51d71f6a4c8918f482f', 'eUSD'),
        ('0x000000000000000000000000320623b8e4ff03373931769a31fc52a4e78b5d70', 'RSR'),
        ('0x0000000000000000000000000d86883faf4ffd7aeb116390af37746f45b6f378', 'USD3'),
        ('0x000000000000000000000000fc0b1eef20e4c68b3dcf36c4537cfa7ce46ca70b', 'USDC'),
        ('0x000000000000000000000000ab36452dbac151be02b16ca17d8919826072f64a', 'RSR'),
        ('0x000000000000000000000000cb327b99ff831bf8223cced12b1338ff3aa322ff', 'bsdETH'),
        ('0x000000000000000000000000cc7ff230365bd730ee4b352cc2492cedac49383e', 'hyUSD')
    )
)
, revenue_with_symbol as (
    select
        r.date,
        r.contract_address,
        r.data_value,
        r.tx_hash,
        a.symbol
    from revenue_data r
    left join address_to_symbol a
    on r.contract_address = a.contract_address
)

-- === Dynamic price tables per token ===

, hyusd_price as (
    {{ get_coingecko_metrics("high-yield-usd-base") }}
)
, ethp_price as (
    {{ get_coingecko_metrics("reserve-protocol-eth-plus") }}
)
, eusd_price as (
    {{ get_coingecko_metrics("electronic-usd") }}
)
, rsr_price as (
    {{ get_coingecko_metrics("reserve-rights-token") }}
)
, usd3_price as (
    {{ get_coingecko_metrics("web-3-dollar") }}
)
, usdc_price as (
    {{ get_coingecko_metrics("usd-coin") }}
)
, bsdeth_price as (
    {{ get_coingecko_metrics("based-eth") }}
)

-- === Combine all price data into one ===

, all_prices as (
    select 'hyUSD' as symbol, * from hyusd_price
    union all
    select 'ETH+' as symbol, * from ethp_price
    union all
    select 'eUSD' as symbol, * from eusd_price
    union all
    select 'RSR' as symbol, * from rsr_price
    union all
    select 'USD3' as symbol, * from usd3_price
    union all
    select 'USDC' as symbol, * from usdc_price
    union all
    select 'bsdETH' as symbol, * from bsdeth_price
)

select
    r.date
    , r.symbol
    , sum(r.data_value * coalesce(p.price, 0)) as ecosystem_revenue
from revenue_with_symbol r
left join all_prices p
    on r.symbol = p.symbol and r.date = p.date
group by r.date, r.symbol
order by r.date desc, r.symbol