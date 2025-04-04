{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="RAYDIUM",
        database="raydium",
        schema="core",
        alias="ez_metrics",
    )
}}

with buyback_from_pair as (
    select date, sum(coalesce(buyback, 0)) as buyback_native
    from {{ ref("ez_raydium_metrics_by_pair") }}
    group by 1
)
, buyback as ( --> buyback by tracking the direct RAY deposit, due to RAY price, will be the most accurate for amount usd, but it's less frequent
    SELECT
        date,
        token_mint_address,
        sum(amount_raw) as buyback_native
    FROM {{ ref("fact_raydium_buyback") }}
    GROUP BY 1,2
)
, treasury as (
    SELECT
        date,
        token_mint_address,
        sum(amount_raw) as treasury_fees_native
    FROM {{ ref("fact_raydium_treasury_fees") }}
    GROUP BY 1,2
)
, pool_creation as (
    SELECT
        date,
        token_mint_address,
        sum(amount_raw) as pool_creation_fees_native
    FROM {{ ref("fact_raydium_pool_creation_fees") }}
    GROUP BY 1,2
)
, trading_volume as (
    select 
        date
        , trading_volume
        , unique_traders
        , number_of_swaps
    from {{ ref("fact_raydium_trading_volumes") }}
)

, tvl as (
    select 
        t.date, 
        avg(t.tvl) as tvl
    from pc_dbt_db.prod.fact_defillama_protocol_tvls t
    join pc_dbt_db.prod.fact_defillama_protocols p on p.id = t.DEFILLAMA_PROTOCOL_ID and p.name ilike '%raydium%'
    group by 1  -- duplicate entry in source tvl table causing issues with incremental merge
)

, price_data as (
    {{ get_coingecko_metrics("raydium") }}
)
, date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date between '2021-03-17' and to_date(sysdate())
)

select 
    ds.date
    , v.trading_volume
    
    , bfp.buyback_native / 0.12 + coalesce(c.pool_creation_fees_native * pc.price, 0) as fees -- trading fee + pool creation
    
    , coalesce(b.buyback_native * pb.price, 0) + coalesce(t.treasury_fees_native * pt.price, 0) as revenue

    , coalesce(t.treasury_fees_native * pt.price, 0) as treasury_fees
    , coalesce(t.treasury_fees_native, 0) as treasury_fees_native 
    , coalesce(c.pool_creation_fees_native, 0) as pool_creation_fees_native
    , v.unique_traders
    , v.number_of_swaps
    , b.buyback_native * pb.price as buyback

    -- Standardized Metrics
    -- Market Metrics
    , price_data.price 
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume

    -- Usage/Sector Metrics
    , v.unique_traders as spot_dau
    , v.number_of_swaps as spot_txns
    , v.trading_volume as spot_volume
    , coalesce(tvl.tvl,
            LAST_VALUE(tvl.tvl IGNORE NULLS) OVER (ORDER BY v.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as tvl 

    -- Money Metrics
    , bfp.buyback_native / 0.12 as trading_fees -- total_trading_fee = buyback (12%) + treasury (4%) + LP(84%); using buyback from token pair as it's more frequent than actual deposit of RAY
    , coalesce(c.pool_creation_fees_native * pc.price, 0) as pool_creation_fees -- pool creation
    , trading_fees + coalesce(c.pool_creation_fees_native * pc.price, 0) as gross_protocol_revenue
    , trading_fees * 0.12 as buyback_cash_flow
    , trading_fees * 0.04 as treasury_cash_flow
    , trading_fees * 0.84 as service_cash_flow
    , b.buyback_native * pb.price as buybacks
    , b.buyback_native   as buyback_native

    -- Other Metrics
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv 
from date_spine ds
left join trading_volume v using(date)
left join price_data using (date)
left join buyback as b using (date)
left join tvl using (date)
left join buyback_from_pair as bfp using (date)
left join treasury t using (date) 
left join pool_creation c using (date) 
left join SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY pb on pb.token_address = b.token_mint_address
        and pb.hour = b.date and pb.blockchain = 'solana'
left join SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY pt on pt.token_address = t.token_mint_address
        and pt.hour = t.date and pt.blockchain = 'solana'
left join SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY pc on pc.token_address = c.token_mint_address
        and pc.hour = c.date and pc.blockchain = 'solana'
where ds.date < to_date(sysdate())
order by 1 desc 


/*

**Raydium programs**
- Standard AMM (CP-Swap, New) -> CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C
- OpenBook AMM ->675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 (Raydium Liquidity Pool V4)
- Stable Swap AMM -> 5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h (raydium liquidity pool program id v5)
- Concentrated Liquidity (CLMM) -> CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK (raydium concentrated liquidity)

!! flipsie ez_dex_swaps does not hae CPMM

--------- 

** Buyback ** 
- Standard AMM (OpenBook, AMMv4) pools: PNLCQcVCD26aC7ZWgRyr5ptfaR7bBrWdTFgRWwu2tvF
- Concentrated Liquidity (CLMM) pools: projjosVCPQH49d5em7VYS7fJZzaqKixqKtus7yk416
- CP-Swap (CPMM) pools: ProCXqRcXJjoUd1RNoo28bSizAA6EEqt9wURZYPDc5u
[alternatively can also track the burn address DdHDoz94o2WJmD9myRobHCwtx1bESpHTd4SSPe6VEZaz, but won't get token then]

--> Need to look for transfers to these addresses that are RAY token (4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R)

--------- 

** Treasury **
The remaining 4% treasury fees from CLMM pool trades are auto-swapped to USDC and transferred to CHynyGLd4fDo35VP4yftAZ9724Gt49uXYXuqmdWtt68F (previously sent to 6pJu). Treasury fees, also denominated in USDC, from CP-Swap (CPMM) pool trades are transferred to FS3HipLdf13nhaeN9NhRfGr1cnH84GdNEam3WHhUXVYJ. These accounts are controlled by the protocol multisig.

USDC fees for treasury:
- CLMM pools; FundHfY8oo8J9KYGyfXFFuQCHe7Z1VBNmsj84eMcdYs4
- CP-Swap pools: FUNDduJTA7XcckKHKfAoEnnhuSud2JUCUZv6opWEjrBU
--> they send USDC to GThUX1Atko4tqhN2NaiTazWSeFWMuiUvfFnyJyUghFMJ, which is Raydium Upgrade Authority 
-- USDC address EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v

--------- 

** Pool Creation **
-- CLMM = no 
-- CPMM DNXgeM9EiiaAbaWvwjHj9fQQLAX5ZsfHyvmYUNRAdNC8 (0.15SOL)
-- AMMv4 7YttLkHDoNj9wyDur5pM1ejNaAvT9X4eqaYcHQqtj2G5 (0.4SOL)


*/