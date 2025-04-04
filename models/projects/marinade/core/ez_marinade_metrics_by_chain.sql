{{
    config(
        materialized="table",
        snowflake_warehouse="marinade",
        database="marinade",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with tvl as (
    select
        date
        , liquid
        , native
        , tvl
    from {{ ref("fact_marinade_tvl") }}
),
dau as (
    select
        date
        , dau
        , txns
    from {{ ref("fact_marinade_dau_txns") }}
),
v1_fees as (
    select
        date
        , coalesce(unstaking_fees, 0) as unstaking_fees
        , coalesce(fees_native, 0) as fees_native
    from {{ ref("fact_marinade_v1_fees") }}
),
v2_fees as (
    select
        date
        , fees_native   
    from {{ ref("fact_marinade_fees") }}
),
fees as (
    select
        coalesce(v1_fees.date, v2_fees.date) as date,
        coalesce(v1_fees.unstaking_fees, 0) as unstaking_fees,
        coalesce(v1_fees.fees_native, 0) + coalesce(v2_fees.fees_native, 0) as fees_native,
    from v1_fees
    full outer join v2_fees using (date)
),
circulating_supply as (
    select
        date
        , circulating_supply
    from {{ ref("fact_marinade_circulating_supply") }}
),
price as (
    select * from ({{ get_coingecko_price_with_latest("solana") }}) 
),
market_metrics as (
    {{ get_coingecko_metrics("marinade") }}
)

select
    date
    , 'solana' as chain

    --Old metrics needed for compatibility
    , liquid
    , native
    , tvl as tvl_native
    , tvl * price as tvl
    , dau
    , txns
    , fees_native
    -- v1 fees (2024-08-17 and before) - commission + unstaking fees, v2 fees (2024-08-18 and after) - validator bids
    -- v2 fees - 100% goes to the protocol
    , fees_native * price as fees
    -- v1 - unstaking fees 75% goes to LP (Fees to Suppliers), 25% goes to treasury (Fees to Holders)
    , unstaking_fees
    , unstaking_fees * 0.75 as supply_side_fee
    , case when 
        date < '2024-08-18' then unstaking_fees * 0.25 + fees
    -- when v2 fees are active, 100% goes to the protocol
        else fees 
    end as revenue
    , circulating_supply
    , price

    --Standardized Metrics
    , unstaking_fees as unstaking_fees
    , fees_native as total_protocol_fees_native
    , total_protocol_fees_native * price as total_protocol_fees
    , case when 
        date < '2024-08-18' then unstaking_fees * 0.25 + fees
    -- when v2 fees are active, 100% goes to the protocol
        else fees 
    end as gross_protocol_revenue
    , unstaking_fees * 0.75 as supply_side_revenue
    , dau as lst_dau
    , txns as lst_txns
    , liquid as tvl_liquid_stake
    , native as tvl_native_stake
    , tvl as tvl_native
    , tvl * price as tvl

    --Market Metrics
    , circulating_supply as circulating_supply
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv
    , market_metrics.token_volume
from tvl
left join dau using (date)
left join fees using (date)
left join circulating_supply using (date)
left join price using (date)
order by date desc