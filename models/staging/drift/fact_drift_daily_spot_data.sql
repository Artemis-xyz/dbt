{{ config(materialized="table") }}
SELECT 
    DATE_TRUNC('DAY', TO_TIMESTAMP(value:timestamp::string, 'YYYY/MM/DD HH24:MI:SS'))::date AS date,
    value:market_name::varchar as market,
    AVG(value:deposit_rate::float) as daily_avg_deposit_rate,
    AVG(value:borrow_rate::float) as daily_avg_borrow_rate,
    AVG(value:"utilization"::float) as daily_avg_utilization,
    AVG(value:annualized_borrow_revenue::float) as daily_avg_annualized_borrow_revenue,
    AVG(value:rev_pool_tokens::float) as daily_avg_revenue_pool_tokens,
    AVG(value:spot_fee_pool_tokens::float) as daily_avg_spot_fee_pool_tokens,
    AVG(value:protocol_balance::float) as daily_avg_protocol_balance,
    AVG(value:user_balance::float) as daily_avg_user_balance,
    AVG(value:staking_apr::float) as daily_avg_staking_apr
FROM {{ source("PROD_LANDING", "raw_drift_spot_market_data") }},
    lateral flatten(input => parse_json(source_json))
where value:market_name::varchar is not null
GROUP BY 1, 2
