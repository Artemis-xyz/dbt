{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with fees_and_revs as (
    select
        date,
        chain,
        sum(revenue_usd) as revenue_usd
    from {{ ref('fact_liquity_fees_and_revs') }}
    group by 1, 2
)
, token_incentives as (
    select
        date,
        chain,
        sum(token_incentives) as token_incentives
    from {{ ref('fact_liquity_token_incentives') }}
    group by 1, 2
)
, treasury as (
    SELECT
        date,
        chain,
        sum(native_balance) as treasury_value,
        SUM(
            CASE WHEN token = 'LQTY'
                THEN native_balance
            END
        ) AS treasury_value_native,
        SUM(
            CASE WHEN token <> 'LQTY'
                THEN native_balance
            END
        ) AS net_treasury_value
    FROM {{ ref('fact_liquity_treasury') }}
    GROUP BY 1, 2
)
, tvl as (
    select
        date,
        chain,
        sum(tvl_usd) as tvl
    from {{ ref('fact_liquity_tvl') }}
    group by 1, 2
) 
, outstanding_supply as (
    select
        date,
        chain,
        sum(outstanding_supply) as outstanding_supply
    from {{ ref('fact_liquity_outstanding_supply') }}
    group by 1, 2
)
, date_chain_spine as (
    select
        date,
        chain
    from {{ ref('dim_date_spine') }}
    cross join (select distinct chain from tvl
                union
                select distinct chain from outstanding_supply
                union
                select distinct chain from fees_and_revs)
    where date between '2021-04-05' and to_date(sysdate())
)

select
    dcs.date,
    dcs.chain,
    
    -- Fees and revenue
    fr.revenue_usd as fees,
    fr.revenue_usd as revenue,
    ti.token_incentives,
<<<<<<< HEAD
=======
    ti.token_incentives as expenses,
>>>>>>> 78bb945ba67884898bd870efea26d09c447b26d1
    fr.revenue_usd - ti.token_incentives as protocol_earnings,

    -- Treasury
    treasury.treasury_value,
    treasury.treasury_value_native,
    treasury.net_treasury_value,

    -- TVL
    tvl.tvl,
    os.outstanding_supply
from date_chain_spine dcs
left join tvl using (date, chain)
left join outstanding_supply os using (date, chain)
left join fees_and_revs fr using (date, chain)  
left join treasury using (date, chain)
left join token_incentives ti using (date, chain)