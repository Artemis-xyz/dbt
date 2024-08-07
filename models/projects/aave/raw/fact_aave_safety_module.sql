{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_safety_module",
    )
}}


{% set start_date = '2018-01-01' %}
{% set end_date = modules.datetime.date.today().isoformat() %}
{% set days_between = (modules.datetime.datetime.strptime(end_date, '%Y-%m-%d') - modules.datetime.datetime.strptime(start_date, '%Y-%m-%d')).days %}
{% set rowcount = days_between + 1 %}  -- Add 1 to include both start and end dates

with
stkAAVE as (
    select
        block_timestamp::date as date
        , lower('0x4da27a545c0c5b758a6ba100e3a049001de870f5') as token
        , case 
            when to_address = '0x0000000000000000000000000000000000000000' then -amount
            when from_address = '0x0000000000000000000000000000000000000000' then amount
        end as mint
    from ethereum_flipside.core.ez_token_transfers
    where lower(contract_address) = lower('0x4da27a545c0c5b758a6ba100e3a049001de870f5')
        and (
            to_address = '0x0000000000000000000000000000000000000000'
            or from_address = '0x0000000000000000000000000000000000000000'
        )
)
, stkABPT_mints as (
    select
        block_timestamp::date as date
        , lower('0xa1116930326d21fb917d5a27f1e9943a9595fb47') as token
        , case 
            when to_address = '0x0000000000000000000000000000000000000000' then -amount
            when from_address = '0x0000000000000000000000000000000000000000' then amount
        end as mint
    from ethereum_flipside.core.ez_token_transfers
    where lower(contract_address) = lower('0xa1116930326d21fb917d5a27f1e9943a9595fb47')
        and (
            to_address = '0x0000000000000000000000000000000000000000'
            or from_address = '0x0000000000000000000000000000000000000000'
        )
)
, stkGHO_mints as (
    select
        block_timestamp::date as date
        , lower('0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d') as token
        , case 
            when to_address = '0x0000000000000000000000000000000000000000' then -amount
            when from_address = '0x0000000000000000000000000000000000000000' then amount
        end as mint
    from ethereum_flipside.core.ez_token_transfers
    where lower(contract_address) = lower('0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d')
        and (
            to_address = '0x0000000000000000000000000000000000000000'
            or from_address = '0x0000000000000000000000000000000000000000'
        )
)
, tokens as (
    SELECT lower('0x4da27a545c0c5b758a6ba100e3a049001de870f5') as token 
    UNION 
    SELECT lower('0xa1116930326d21fb917d5a27f1e9943a9595fb47') as token
    UNION 
    SELECT lower('0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d') as token
)
, dt_spine as (
    SELECT '2018-01-01'::date + seq4() AS date
    FROM TABLE(GENERATOR(ROWCOUNT => {{ rowcount }}))
    where date <= to_date(sysdate())
)
, token_days as (
    SELECT tokens.token, dt_spine.date
    from tokens 
    CROSS JOIN dt_spine 
)
, daily_mint as (
    SELECT 
        date
        , token
        , sum(mint) as daily_mint
    FROM (
        SELECT * FROM stkAAVE
        UNION ALL 
        SELECT * FROM stkABPT_mints
        UNION ALL 
        SELECT * FROM stkGHO_mints
    ) a
    GROUP BY date, token
)
, daily_mints_filled as (
    SELECT 
        token_days.token
        , token_days.date
        , COALESCE(daily_mint.daily_mint, 0) as daily_mint
    from token_days 
    LEFT JOIN daily_mint
        ON daily_mint.date = token_days.date
        AND lower(daily_mint.token) = lower(token_days.token)
)
, result as (
    SELECT 
        token
        , date
        , daily_mint
        , sum(daily_mint) over(partition by token order by date) as total_supply
    FROM daily_mints_filled
)
, aave_prices as ({{get_coingecko_price_with_latest("aave")}})
, gho_prices as ({{get_coingecko_price_with_latest("gho")}})
, abpt_prices as ({{get_coingecko_price_with_latest("aave-balancer-pool-token")}})
, prices as (
    select date, '0x4da27a545c0c5b758a6ba100e3a049001de870f5' as token, price 
    from aave_prices
        
    union all 

    select date, '0xa1116930326d21fb917d5a27f1e9943a9595fb47' as token, price 
    from abpt_prices

    union all 

    select date, '0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d' as token, price 
    from gho_prices
)


SELECT 
    result.date
    , 'ethereum' as chain
    , result.token as token_address
    , total_supply as amount_nominal
    , coalesce(prices.price, 0) * total_supply as amount_usd
FROM result
LEFT JOIN prices
    ON prices.token = result.token
    AND prices.date = result.date