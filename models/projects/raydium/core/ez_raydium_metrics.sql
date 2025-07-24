{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="RAYDIUM",
        database="raydium",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with buyback_from_pair as (
    select 
        date
        , sum(coalesce(buyback, 0)) as buyback
        , sum(
                case when program = 'AMMv4' then (buyback / 0.12) * 0.88
                else (buyback/0.12) * 0.84
                end
        ) as lp_fee_allocation
        , sum(
                case when program = 'AMMv4' then (buyback / 0.12) * 0
                else (buyback/0.12) * 0.04
                end
        ) as treasury_fee_allocation
    from {{ ref("fact_raydium_metrics_by_pair") }}
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
, supply_data as (
    select
        date
        , net_supply_change_native
        , gross_emissions_native
        , premine_unlocks_native
        , circulating_supply_native
    from {{ ref("fact_raydium_supply") }}
)

select 
    ds.date
    , v.trading_volume
    
    , bfp.buyback / 0.12 + coalesce(c.pool_creation_fees_native * pc.price, 0) as fees -- trading fee + pool creation
    
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
    , bfp.buyback / 0.12 as spot_fees
    , coalesce(c.pool_creation_fees_native * pc.price, 0) as pool_creation_fees -- pool creation
    , spot_fees + coalesce(c.pool_creation_fees_native * pc.price, 0) as ecosystem_revenue
    , bfp.buyback as buyback_fee_allocation
    , bfp.treasury_fee_allocation as treasury_fee_allocation
    , bfp.lp_fee_allocation as service_fee_allocation
    , b.buyback_native * pb.price as buybacks
    , b.buyback_native   as buyback_native

    -- Supply Metrics
    , supply_data.net_supply_change_native
    , supply_data.gross_emissions_native
    , supply_data.premine_unlocks_native
    , supply_data.circulating_supply_native

    -- Other Metrics
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv 

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
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
left join supply_data using (date)
where true
{{ ez_metrics_incremental('ds.date', backfill_date) }}
and ds.date < to_date(sysdate())
order by 1 desc 
