create or replace table {{ this }} clone {{ ref("agg_daily_stablecoin_breakdown_silver") }}
