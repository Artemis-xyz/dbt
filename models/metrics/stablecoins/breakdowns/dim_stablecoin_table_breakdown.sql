{{ config(materialized='table', unique_key='unique_id', snowflake_warehouse='STABLECOIN_V2_LG_2') }}
with 
    max_date as (
        select max(date) as max_date from {{ref('agg_daily_stablecoin_breakdown')}}
    )
    , stablecoin_metrics_by_chain as (
        select 
            date
            , case 
                when date >= (select dateadd('day', -30, max_date) from max_date) then 0
                else 1
            end as month_number
            , chain
            , symbol
            , coalesce(app, from_address) as app_or_address
            , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
            , sum(stablecoin_supply) as stablecoin_supply
            , sum(stablecoin_daily_txns) as stablecoin_txns
        from {{ref('agg_daily_stablecoin_breakdown')}}
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
            , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
            , sum(stablecoin_txns) as stablecoin_txns
            , max_by(stablecoin_supply, date) as stablecoin_supply
            , ARRAY_AGG(
                OBJECT_CONSTRUCT(
                    'date', date,
                    'value', stablecoin_supply
                )
            ) WITHIN GROUP (ORDER BY date ASC) AS historical_L_30_date
        from stablecoin_metrics_by_chain
        group by app_or_address, chain, symbol, month_number
    )
, data as (
    select
        app_or_address
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
        , historical_L_30_date
    from monthly_data
)
select
    app_or_address
    , chain
    , symbol
    , stablecoin_transfer_volume
    , stablecoin_transfer_volume_pct_chg

    , stablecoin_txns
    , stablecoin_txns_pct_chg
    
    , stablecoin_supply
    , historical_L_30_date
from data
where month_number = 0
order by stablecoin_supply desc