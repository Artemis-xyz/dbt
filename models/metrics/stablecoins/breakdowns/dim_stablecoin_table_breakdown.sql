{{ config(materialized='table', snowflake_warehouse='STABLECOIN_V2_LG') }}
with 
    max_date as (
        select max(date) as max_date from {{ref('agg_daily_stablecoin_breakdown_with_labels_silver')}}
    )
    , stablecoin_metrics_by_chain as (
        select 
            date
            , case 
                when date >= (select dateadd('day', -30, max_date) from max_date) then 0
                else 1
            end as month_number
            , max(icon) as icon
            , chain
            , symbol
            , coalesce(friendly_name, address) as app_or_address
            , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
            -- Hide DQ issues with the stablecoin supply field
            , sum(case when stablecoin_supply < 0 then 0 else stablecoin_supply end) as stablecoin_supply
            , sum(stablecoin_daily_txns) as stablecoin_txns
        from {{ref('agg_daily_stablecoin_breakdown_with_labels_silver')}}
        where date >= (select dateadd('day', -60, max_date) from max_date)
        group by date, chain, symbol, app_or_address, month_number
        order by date
    )
    , monthly_data as (
        select
            app_or_address
            , chain
            , symbol
            , month_number
            , max(icon) as icon
            , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
            , sum(stablecoin_txns) as stablecoin_txns
            , max_by(stablecoin_supply, date) as stablecoin_supply
            , ARRAY_AGG(
                ARRAY_CONSTRUCT(
                    DATE_PART(EPOCH_SECOND, date::TIMESTAMP_NTZ),
                    round(coalesce(stablecoin_supply, 0)::NUMBER(38, 18), 2)
                )
            ) WITHIN GROUP (ORDER BY date ASC) AS historical_L_30_stablecoin_supply
        from stablecoin_metrics_by_chain
        group by app_or_address, chain, symbol, month_number
    )
, data as (
    select
        ARRAY_CONSTRUCT(
            app_or_address,
            coalesce(icon, '')
        ) as name
        , icon
        , app_or_address
        , chain
        , symbol
        , month_number
        , stablecoin_transfer_volume
        , ((stablecoin_transfer_volume - LAG(stablecoin_transfer_volume) OVER (partition by app_or_address, chain, symbol ORDER BY month_number desc)) / 
            NULLIF(LAG(stablecoin_transfer_volume) OVER (partition by app_or_address, chain, symbol  ORDER BY month_number desc), 0)) 
        * 100 AS stablecoin_transfer_volume_pct_chg
    
        , stablecoin_txns
        , ((stablecoin_txns - LAG(stablecoin_txns) OVER (partition by app_or_address, chain, symbol  ORDER BY month_number desc)) / 
            NULLIF(LAG(stablecoin_txns) OVER (partition by app_or_address, chain, symbol  ORDER BY month_number desc), 0)) 
        * 100 AS stablecoin_txns_pct_chg
        
        , stablecoin_supply
        , historical_L_30_stablecoin_supply
    from monthly_data
)
select
    name
    , app_or_address
    , icon
    , chain
    , symbol
    , stablecoin_transfer_volume
    , stablecoin_transfer_volume_pct_chg

    , stablecoin_txns
    , stablecoin_txns_pct_chg
    
    , stablecoin_supply
    , historical_L_30_stablecoin_supply
from data
where month_number = 0
order by stablecoin_supply desc