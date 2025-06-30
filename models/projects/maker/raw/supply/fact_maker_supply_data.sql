{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
    )
}}


with treasury_pivoted as (
    SELECT
        date,
        SUM(IFF(token='MKR', amount_native, 0)) as amount_mkr, 
        SUM(IFF(token='SKY', amount_native, NULL)) as amount_sky
    FROM
        {{ ref('fact_treasury_mkr') }}
    GROUP BY 1
)
SELECT
    m.date, 
    m.total_supply_mkr,
    coalesce(amount_mkr, 0) as treasury_mkr,
    m.total_supply_mkr - treasury_mkr as issued_supply_mkr,
    issued_supply_mkr as circulating_supply_mkr,

    
    c.balance_native as converter_sky_balance,
    s.total_supply_sky - coalesce(converter_sky_balance, 0) as total_supply_sky_less_converter,
    amount_sky as treasury_sky,
    s.total_supply_sky - coalesce(converter_sky_balance, 0) - treasury_sky as issued_supply_sky_less_converter,
    issued_supply_sky_less_converter as circulating_supply_sky_less_converter,

    m.total_supply_mkr + coalesce(s.total_supply_sky/24000, 0) - coalesce(converter_sky_balance/24000, 0) as total_supply_mkr_adj,
    treasury_mkr + coalesce(treasury_sky/24000, 0) as treasury_mkr_adj,
    total_supply_mkr_adj - treasury_mkr_adj as issued_supply_mkr_adj,
    
    m.total_supply_mkr * 24000 + total_supply_sky_less_converter as total_supply_sky_adj,
    treasury_mkr * 24000 + treasury_sky as treasury_sky_adj,
    total_supply_sky_adj - treasury_sky_adj as issued_supply_sky_adj,

    
    issued_supply_mkr_adj as circulating_supply_mkr_adj,
    issued_supply_sky_adj as circulating_supply_sky_adj
    
    
FROM
    {{ ref('fact_mkr_total_supply') }} m
LEFT JOIN {{ ref('fact_sky_total_supply') }} s USING(DATE)
LEFT JOIN treasury_pivoted USING(DATE)
LEFT JOIN {{ ref('fact_mkr_sky_converter_balance') }} c USING(date)
ORDER BY 1 DESC

