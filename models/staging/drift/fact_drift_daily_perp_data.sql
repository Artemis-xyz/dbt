{{ config(materialized="table") }}
SELECT 
    DATE_TRUNC('DAY', TO_TIMESTAMP(value:timestamp::string, 'YYYY/MM/DD HH24:MI:SS'))::date AS date,
    value:market_name::varchar as market,
    AVG(value:predicted_funding_rate::float) as daily_avg_predicted_funding_rate,
    AVG(value:last_funding_rate::float) as daily_avg_last_funding_rate,
    AVG(value:"24h_avg_funding_rate"::float) as daily_avg_24h_funding_rate,
    AVG(value:pnl_pool::float) as daily_avg_pnl_pool,
    AVG(value:fee_pool::float) as daily_avg_fee_pool,
    AVG(value:oi::float) as daily_avg_open_interest,
    AVG(value:oi_net::float) as daily_avg_net_open_interest,
    AVG(value:net_user_pnl::float) as daily_avg_net_user_pnl,
    AVG(value:excess_pnl::float) as daily_avg_excess_pnl,
    AVG(value:est_funding_dollars::float) as daily_avg_est_funding_dollars
FROM {{ source("PROD_LANDING", "raw_drift_perp_market_data") }} ,
    lateral flatten(input => parse_json(source_json))
where value:market_name::varchar is not null
GROUP BY 1, 2
