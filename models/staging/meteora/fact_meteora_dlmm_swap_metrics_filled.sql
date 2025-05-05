{{
    config(
        materialized="incremental",
        unique_key="date",
        alias="fact_meteora_dlmm_swap_metrics_filled",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

with
    -- Original metrics from the source table
    spot_metrics as (
        select
            date,
            unique_traders,
            number_of_swaps,
            trading_volume
        from {{ref('fact_meteora_dlmm_swap_metrics')}}
    )
    
    -- Generate a continuous date range from min to max date
    , date_range as (
        SELECT 
            date 
        FROM {{ ref('dim_date_spine') }}
        WHERE date BETWEEN (SELECT MIN(date) FROM spot_metrics) AND TO_DATE(SYSDATE())
        ORDER BY date
    )
    
    -- Apply forward fill to each metric
    , forward_filled_metrics as (
        select
            date_range.date,
            coalesce(
                spot_metrics.unique_traders, 
                LAST_VALUE(spot_metrics.unique_traders IGNORE NULLS) OVER (
                    ORDER BY date_range.date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) as unique_traders,
            
            coalesce(
                spot_metrics.number_of_swaps, 
                LAST_VALUE(spot_metrics.number_of_swaps IGNORE NULLS) OVER (
                    ORDER BY date_range.date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) as number_of_swaps,
            
            coalesce(
                spot_metrics.trading_volume, 
                LAST_VALUE(spot_metrics.trading_volume IGNORE NULLS) OVER (
                    ORDER BY date_range.date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) as trading_volume
        from date_range
        left join spot_metrics using (date)
    )

select
    date,
    unique_traders,
    number_of_swaps,
    trading_volume
from forward_filled_metrics
{% if is_incremental() %}
    where date > (select max(date) from {{ this }})
{% endif %}
order by date