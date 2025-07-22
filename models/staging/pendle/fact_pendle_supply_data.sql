{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE"
    )
}}

with emissions as (
    SELECT
        date,
        sum(amt_pendle) as emissions
    FROM
        {{ ref('fact_pendle_token_incentives_by_chain_silver')}}
    GROUP BY 1
)
, ve_pendle_locked as (
    SELECT
        date,
        sum(balance_native) as ve_pendle_balance
    FROM {{ ref('fact_pendle_vependle_balance') }}
    WHERE contract_address = '0x808507121b80c02388fad14726482e061b8da827'
    GROUP BY 1
)
, total_supply as (
    SELECT
        block_timestamp::date as date,
        sum(amount) as minted_amount
    FROM
        ethereum_flipside.core.ez_token_transfers
    WHERE 
        FROM_ADDRESS = lower('0x0000000000000000000000000000000000000000')
        AND contract_address = lower('0x808507121B80c02388fAd14726482e061B8da827')
    GROUP BY 1
)
, ecosystem_fund_holdings as (
    SELECT
        date,
        SUM(balance_native) as ecosystem_fund_balance
    FROM {{ ref('fact_pendle_treasury') }}
    WHERE contract_address = '0x808507121b80c02388fad14726482e061b8da827'
    GROUP BY 1
)
, date_spine as (
    SELECT
        date
    FROM
        {{ ref('dim_date_spine')}}
    WHERE date between '2021-04-27' and to_date(sysdate()) -- TGE to present
)
SELECT
    ds.date
    , coalesce(ecosystem_fund_balance, 0) as foundation_balance
    , coalesce(advisors + liquidity_boostrapping + investors + team, 0) as unlocks_native
    , 111.5 * 1e6  as total_unlock_allocation
    , 46 * 1e6 as foundation_allocation
    , foundation_allocation-foundation_balance as foundation_emitted
    , sum(unlocks_native) over (order by date asc) as unlocks_native_cumulative
    , coalesce(emissions, 0) as emissions_native
    , sum(emissions_native) over (order by date asc) as emissions_native_cumulative
    , coalesce(ve_pendle_balance, 0) as pendle_locked

    , sum(minted_amount) over (order by date asc) as total_supply_native
    , total_unlock_allocation + emissions_native_cumulative - pendle_locked + foundation_emitted as issued_supply_native
    , unlocks_native_cumulative + emissions_native_cumulative - pendle_locked + foundation_emitted as circulating_supply_native
FROM date_spine ds
LEFT JOIN {{ source("MANUAL_STATIC_TABLES", "pendle_unlocks_seed") }} using(date)
LEFT JOIN emissions using(date)
LEFT JOIN ve_pendle_locked using(date)
LEFT JOIN total_supply using(date)
LEFT JOIN ecosystem_fund_holdings using(date)