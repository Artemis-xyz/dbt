{{
    config(
        materialized='table',
        snowflake_warehouse='USUAL'
    )
}}

WITH usualBurned AS (
    SELECT 
        DATE(block_timestamp) AS date
        , SUM(TRY_CAST(raw_amount_precise AS numeric) / 1e18) AS daily_burned
    FROM ethereum_flipside.core.ez_token_transfers
    WHERE 
        lower(contract_address) = lower('0xC4441c2BE5d8fA8126822B9929CA0b81Ea0DE38E')
        AND lower(to_address) = '0x0000000000000000000000000000000000000000'
    GROUP BY 1
),
usualToTreasury AS (
    SELECT
        DATE(block_timestamp) AS date
        , SUM(TRY_CAST(raw_amount_precise AS numeric) / 1e18) AS daily_treasury
    FROM ethereum_flipside.core.ez_token_transfers
    WHERE
        lower(contract_address) = lower('0xC4441c2BE5d8fA8126822B9929CA0b81Ea0DE38E')
        AND lower(to_address) = lower('0x81ad394c0fa87e99ca46e1aca093bee020f203f4')
    GROUP BY 1
),
agg_data AS (
    SELECT
        t.date
        , COALESCE(t.daily_treasury, 0) AS daily_treasury
        , COALESCE(b.daily_burned, 0) AS daily_burned
        , SUM(COALESCE(t.daily_treasury, 0)) OVER (ORDER BY t.date) AS cumulative_treasury
        , SUM(COALESCE(b.daily_burned, 0)) OVER (ORDER BY t.date) AS cumulative_burned
    FROM usualToTreasury t
    LEFT JOIN usualBurned b USING (date)
),
net_balance AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS date
        , SUM(CASE 
            WHEN LOWER(decoded_log:"from") = '0x0000000000000000000000000000000000000000' 
            THEN decoded_log:"value" / 1e18
            WHEN LOWER(decoded_log:"to") = '0x0000000000000000000000000000000000000000' 
            THEN -decoded_log:"value" / 1e18
            ELSE 0
        END) AS tokens_mint
    FROM ethereum_flipside.core.ez_decoded_event_logs
    WHERE 
        LOWER(contract_address) = LOWER('0xC4441c2BE5d8fA8126822B9929CA0b81Ea0DE38E')
        AND event_name = 'Transfer'
    GROUP BY 1
)

, cumulative_supply AS (
    SELECT 
        CAST(date AS DATE) AS date
        , SUM(tokens_mint) OVER (ORDER BY date) AS cumulative_supply
        , tokens_mint AS daily_supply
    FROM net_balance
)

, investors_balances as (
    select
        date(block_timestamp) as date
        , max(user_address) as user_address
        , max(contract_address) as contract_address
        , max_by(balance / 1e18, date) as balance
    from ethereum_flipside.core.fact_token_balances
    where
        lower(contract_address) = lower('0xC4441c2BE5d8fA8126822B9929CA0b81Ea0DE38E')
        and lower(user_address) = lower('0xa55AF35E5F4bb6A82E0A290570BcE38Ce2757d37')
    group by 1
)

, current_usual_balance as (
    select
        ibc.date
        , ibc.balance
        , cs.daily_supply
        , cs.cumulative_supply
        , cs.cumulative_supply - ibc.balance as circulating_supply_native
    from investors_balances ibc
    left join cumulative_supply cs on ibc.date = cs.date
)

, net_supply_change as (
    select 
        c.date
        , coalesce(c.daily_supply, 0) as gross_emissions_native
        , coalesce(c.cumulative_supply, 0) as cumulative_supply
        , coalesce(a.daily_treasury, 0) as daily_treasury
        , coalesce(a.cumulative_treasury, 0) as cumulative_treasury
        , coalesce(a.daily_burned, 0) as burns_native
        , coalesce(a.cumulative_burned, 0) as cumulative_burns_native
        , coalesce(c.circulating_supply_native, 0) as circulating_supply_native
        , coalesce(gross_emissions_native, 0) - coalesce(burns_native, 0) as net_supply_change_native
        -- 1/3 goes to usual* holders, 1/3 goes to usualx holders, 1/3 goes to burn
        , a.daily_treasury * 0.5 as daily_treasury_usualx
        , a.daily_treasury * 0.5 as daily_treasury_usualstar
    from current_usual_balance c
    left join agg_data a on c.date = a.date
    order by c.date desc
)

select 
    date
    , gross_emissions_native
    , burns_native
    , net_supply_change_native
    , circulating_supply_native
    , cumulative_supply
    , daily_treasury
    , cumulative_treasury
    , daily_treasury_usualx
    , daily_treasury_usualstar
from net_supply_change
