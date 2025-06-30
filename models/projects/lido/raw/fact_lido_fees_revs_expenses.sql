{{
    config(
        materialized="table",
        snowflake_warehouse="LIDO",
        database="lido",
        schema="raw",
        alias="fact_fees_revs_expenses",
    )
}}

with steth_prices as (
    SELECT
        hour,
        price as price
    FROM
        ethereum_flipside.price.ez_prices_hourly
    WHERE token_address = lower('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
)
, fees_revs_expenses as (
    select
        date(block_timestamp) as date
        , 'ethereum' as chain
        , 'stETH' as token
        , sum(raw_amount_precise::number / 1e18 / (f.treasury_fee_pct + f.insurance_fee_pct)) as total_staking_yield_native
        , sum(raw_amount_precise::number / 1e18 / (f.treasury_fee_pct + f.insurance_fee_pct) * p.price) as total_staking_yield_usd
        , AVG(f.treasury_fee_pct) as treasury_fee_pct
        , AVG(f.insurance_fee_pct) as insurance_fee_pct
        , AVG(f.operators_fee_pct) as operators_fee_pct
    from
        ethereum_flipside.core.ez_token_transfers t
    left join steth_prices p on date_trunc('hour', t.block_timestamp) = p.hour
    left join {{ref('fact_lido_fee_split')}} f on t.block_timestamp::date = f.date
    where
        contract_address = lower('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
        and from_address = lower('0x0000000000000000000000000000000000000000')
        and to_address = lower('0x3e40d73eb977dc6a537af587d48316fee66e9c8c')
        and origin_function_signature <> lower('0xf98a4eca')
    group by 1
)
, mev as (
    SELECT
        date(hour) as date
        , COALESCE(pc_dbt_db.prod.HEX_TO_INT(data)::number,0) / 1e18 as mev_priority_fees_amount_eth
        , COALESCE(pc_dbt_db.prod.HEX_TO_INT(data)::number,0) / 1e18 * COALESCE(p.price, 0) as mev_priority_fees_amount_usd
    FROM
        ethereum_flipside.core.fact_event_logs l
        LEFT JOIN ethereum_flipside.price.ez_prices_hourly p ON p.hour = date_trunc('hour', l.block_timestamp)
        AND p.symbol = 'ETH' AND is_native = True
    WHERE
        topics [0] = '0xd27f9b0c98bdee27044afa149eadcd2047d6399cb6613a45c5b87e6aca76e6b5'
)
SELECT
    f.date
    -- NATIVE
    , 'ETH' as symbol
    , COALESCE(f.total_staking_yield_native, 0) - COALESCE(m.mev_priority_fees_amount_eth, 0) AS block_rewards_native
    , COALESCE(m.mev_priority_fees_amount_eth, 0) AS mev_priority_fees_native
    , COALESCE(f.total_staking_yield_native, 0) AS total_staking_yield_native
    , COALESCE(f.total_staking_yield_native, 0) * (operators_fee_pct + treasury_fee_pct + insurance_fee_pct) AS fees_native
    , COALESCE(f.total_staking_yield_native, 0) * operators_fee_pct as validator_fee_allocation_native
    , COALESCE(f.total_staking_yield_native, 0) * (treasury_fee_pct + insurance_fee_pct) as treasury_fee_allocation_native
    , COALESCE(f.total_staking_yield_native, 0) * (treasury_fee_pct + insurance_fee_pct) as protocol_revenue_native
    , block_rewards_native * 0.90 as primary_supply_side_revenue_native
    , mev_priority_fees_amount_eth * 0.90 as secondary_supply_side_revenue_native
    , total_staking_yield_native * 0.90 as total_supply_side_revenue_native
    -- USD
    , COALESCE(f.total_staking_yield_usd, 0) - COALESCE(m.mev_priority_fees_amount_usd, 0) AS block_rewards
    , COALESCE(m.mev_priority_fees_amount_usd, 0) AS mev_priority_fees
    , COALESCE(f.total_staking_yield_usd, 0) AS total_staking_yield
    , COALESCE(f.total_staking_yield_usd, 0) * (operators_fee_pct + treasury_fee_pct + insurance_fee_pct) AS fees
    , COALESCE(f.total_staking_yield_usd, 0) * operators_fee_pct as validator_fee_allocation
    , COALESCE(f.total_staking_yield_usd, 0) * (treasury_fee_pct + insurance_fee_pct) as treasury_fee_allocation
    , COALESCE(f.total_staking_yield_usd, 0) * (treasury_fee_pct + insurance_fee_pct) as protocol_revenue
    , block_rewards * 0.90 as primary_supply_side_revenue
    , mev_priority_fees * 0.90 as secondary_supply_side_revenue
    , total_staking_yield * 0.90 as total_supply_side_revenue
FROM
    fees_revs_expenses f
LEFT JOIN mev m ON m.date = f.date