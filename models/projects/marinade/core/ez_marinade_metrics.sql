{{
    config(
        materialized="table",
        snowflake_warehouse="marinade",
        database="marinade",
        schema="core",
        alias="ez_metrics",
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
        , remaining_balance as circulating_supply
    from {{ ref("fact_marinade_circulating_supply") }}
),
price as (
    select * from ({{ get_coingecko_price_with_latest("solana") }}) 
)

select
    date
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
        else unstaking_fees * 0.25 
    end as revenue
    , circulating_supply
    , price
from tvl
left join dau using (date)
left join fees using (date)
left join circulating_supply using (date)
left join price using (date)
order by date desc