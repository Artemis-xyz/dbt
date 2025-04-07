{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="RAYDIUM",
        database="raydium",
        schema="core",
        alias="ez_metrics",
    )
}}

with buyback_from_pair as (
    select date, sum(coalesce(buyback, 0)) as buyback
    from {{ ref("ez_raydium_metrics_by_pair") }}
    group by 1
)

, buyback as ( --> buyback by tracking the direct RAY deposit, due to RAY price, will be the most accurate for amount usd, but it's less frequent
    select date_trunc('day', block_timestamp) as day
        , mint as token_mint_address
        , sum(amount) as amount_raw -- RAY amount 
    from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS
    where 1=1
        and mint = '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R' -- RAY
        and tx_to = 'DdHDoz94o2WJmD9myRobHCwtx1bESpHTd4SSPe6VEZaz'
        -- in (
        --     'PNLCQcVCD26aC7ZWgRyr5ptfaR7bBrWdTFgRWwu2tvF' -- AMMv4
        --     , 'projjosVCPQH49d5em7VYS7fJZzaqKixqKtus7yk416' -- CLMM
        --     , 'ProCXqRcXJjoUd1RNoo28bSizAA6EEqt9wURZYPDc5u' -- CPMM 
        -- )
        -- and tx_id = '3RmUQ54hgt8teCdah7B9Wm4Q1EmBwvF1CErZmX9NEGm5Ah2xVwgwd5UQkqvcCKkBx79xCfKEZanzBRiNayLTcZ5f' and block_id = 289035965
    
    {% if is_incremental() %}
        AND block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% else %}
        AND block_timestamp::date >= date('2022-04-22') 
    {% endif %}

    group by 1,2
)

, treasury as (
    select date_trunc('day', block_timestamp) as day
        , mint as token_mint_address
        , sum(amount) as amount_raw -- USDC amount 
    from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS
    where 1=1
        and mint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' -- USDC
        and tx_to = 'GThUX1Atko4tqhN2NaiTazWSeFWMuiUvfFnyJyUghFMJ'
        and tx_from in (
            'FundHfY8oo8J9KYGyfXFFuQCHe7Z1VBNmsj84eMcdYs4' -- CLMM pools
            , 'FUNDduJTA7XcckKHKfAoEnnhuSud2JUCUZv6opWEjrBU' -- CP-Swap pools
            -- there is no treasury fee being sent from AMMv4 pools
        )
    -- and tx_id = 'qksFPkjeiVodccqyQTkCCVaciiDEdzcY6nFEykK3PSYAVNWKAStjtX6wcEutzzmzdNP5azGVKAfH6afXqgSiKF3' and block_id = 289019693

    {% if is_incremental() %}
        AND block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% else %}
        AND block_timestamp::date >= date('2022-04-22') 
    {% endif %}

    group by 1,2
)


, pool_creation as (
    select date_trunc('day', block_timestamp) as day
        , 'So11111111111111111111111111111111111111112' as token_mint_address -- replace native SOL for WSOL mint address to match price later
        , sum(amount) as amount_raw -- SOL amount 
    from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS
    where 1=1
        and mint = 'So11111111111111111111111111111111111111111' -- SOL
        and tx_to in (
         'DNXgeM9EiiaAbaWvwjHj9fQQLAX5ZsfHyvmYUNRAdNC8' -- CPMM (0.15SOL)
         , '7YttLkHDoNj9wyDur5pM1ejNaAvT9X4eqaYcHQqtj2G5' -- AMMv4  (0.4SOL)
         -- CLMM has no pool creation fee to Raydium
        )
    -- and tx_id = '3jo6ZRMHxMFga9ACbgWGmBtjYQP85uMKfFz77HUqv2Z1JSqm32yKoiAWSyim1hqkM7B4xZDvCGEdzrv9qPLtNvpL' and block_id = 289024379

    {% if is_incremental() %}
        AND block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% else %}
        AND block_timestamp::date >= date('2022-04-22') 
    {% endif %}

    group by 1,2

)

, trading_volume as (
    select 
        date as day
        , trading_volume
        , unique_traders
        , number_of_swaps
    from {{ ref("fact_raydium_trading_volumes") }}
    WHERE
    {% if is_incremental() %}
        date >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% else %}
        date >= date('2022-04-22') 
    {% endif %}
)

, tvl as (
    select 
        t.date, 
        avg(t.tvl) as tvl
    from pc_dbt_db.prod.fact_defillama_protocol_tvls t
    join pc_dbt_db.prod.fact_defillama_protocols p on p.id = t.DEFILLAMA_PROTOCOL_ID and p.name = 'Raydium'
    group by 1  -- duplicate entry in source tvl table causing issues with incremental merge
)

, price_data as (
    select date as day
        , price
        , market_cap 
        , fdmc
        , token_turnover_circulating
        , token_turnover_fdv
        , token_volume
    from ({{ get_coingecko_metrics("raydium") }})
    where 1=1
    {% if is_incremental() %}
        AND date >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% else %}
        AND date >= date('2022-04-22') 
    {% endif %}
)

select 
    coalesce(price_data.day, v.day, bfp.date, b.day, tvl.date) as date
    , v.trading_volume
    
    , bfp.buyback / 0.12 + coalesce(c.amount_raw * pc.price, 0) as fees -- trading fee + pool creation
    , bfp.buyback / 0.12 as trading_fees -- total_trading_fee = buyback (12%) + treasury (4%) + LP(84%); using buyback from token pair as it's more frequent than actual deposit of RAY
    
    , coalesce(b.amount_raw * pb.price, 0) + coalesce(t.amount_raw * pt.price, 0) as revenue
    , b.amount_raw * pb.price as buyback
    , b.amount_raw as buyback_native
    , coalesce(t.amount_raw * pt.price, 0) as treasury_fees -- treasury fees can be null
    , coalesce(t.amount_raw, 0) as treasury_fees_native 
    , coalesce(c.amount_raw * pc.price, 0) as pool_creation_fees -- pool creation can be null
    , coalesce(c.amount_raw, 0) as pool_creation_fees_native
    , price_data.price 
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv
    , price_data.token_volume
    , coalesce(tvl.tvl,
            LAST_VALUE(tvl.tvl IGNORE NULLS) OVER (ORDER BY v.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as tvl 
    , v.unique_traders -- not just direct, include aggregator routed
    , v.number_of_swaps
from trading_volume v
left join price_data on price_data.day = v.day
left join tvl on tvl.date = v.day
left join buyback as b on v.day = b.day
left join buyback_from_pair as bfp on v.day = bfp.date
left join treasury t on t.day = v.day 
left join pool_creation c on c.day = v.day 
left join SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY pb on pb.token_address = b.token_mint_address
        and pb.hour = b.day and pb.blockchain = 'solana'
        {% if is_incremental() %}
            AND pb.hour::date >= (select dateadd('day', -3, max(date)) from {{ this }})
        {% else %}
            AND pb.hour::date >= date('2022-04-22') 
        {% endif %}

left join SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY pt on pt.token_address = t.token_mint_address
        and pt.hour = t.day and pt.blockchain = 'solana'
        {% if is_incremental() %}
            AND pt.hour::date >= (select dateadd('day', -3, max(date)) from {{ this }})
        {% else %}
            AND pt.hour::date >= date('2022-04-22') 
        {% endif %}

left join SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY pc on pc.token_address = c.token_mint_address
        and pc.hour = c.day and pc.blockchain = 'solana'
        {% if is_incremental() %}
            AND pc.hour::date >= (select dateadd('day', -3, max(date)) from {{ this }})
        {% else %}
            AND pc.hour::date >= date('2022-04-22') 
        {% endif %}

where coalesce(v.day, bfp.date, b.day, tvl.date) < to_date(sysdate())
order by 1 desc 


/*

**Raydiium prorams**
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