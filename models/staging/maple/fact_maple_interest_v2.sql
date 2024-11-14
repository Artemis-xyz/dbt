{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'MAPLE'
    )
}}

with eth_prices as (
    select * from {{source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly')}}
    where is_native = True
)
, ftlm_interest as (
    select
        block_timestamp,
        tx_hash,
        contract_address,
        pools.pool_name,
        pools.asset,
        CASE WHEN
            contract_address = '0x373bdcf21f6a939713d5de94096ffdb24a406391'
            THEN netinterest_ / POW(10,18) * p.price
        ELSE netinterest_ / POW(10, 6)
        END AS net_interest,
        CASE WHEN
            contract_address = '0x373bdcf21f6a939713d5de94096ffdb24a406391'
            THEN netinterest_ / POW(10,18)
        ELSE netinterest_ / POW(10, 6)
        END AS net_interest_native,
        NULL as platform_fees_usd,
        NULL as platform_fees_native,
        NULL as delegate_fees_usd,
        NULL as delegate_fees_native
    from {{ ref('fact_maple_v2_LoanManager_FundsDistributed') }} f
    left join eth_prices p on p.hour = date_trunc('hour', f.block_timestamp)
    LEFT JOIN {{ ref('dim_maple_pools') }} pools ON f.contract_address = pools.loan_manager 
)

-- OTLM (448 rows)
, otlm_interest as (
    select
        block_timestamp,
        contract_address,
        tx_hash,
        pools.pool_name,
        pools.asset,
        CASE 
            WHEN contract_address in ('0x373bdcf21f6a939713d5de94096ffdb24a406391', '0xe3aac29001c769fafcef0df072ca396e310ed13b')
                THEN netinterest_ / POW(10,18) * p.price
            ELSE netinterest_ / POW(10, 6)
        END AS net_interest_usd,
        CASE 
            WHEN contract_address in ('0x373bdcf21f6a939713d5de94096ffdb24a406391', '0xe3aac29001c769fafcef0df072ca396e310ed13b')
                THEN netinterest_ / POW(10,18)
            ELSE netinterest_ / POW(10, 6)
        END AS net_interest_native,
        CASE 
            WHEN contract_address in ('0x373bdcf21f6a939713d5de94096ffdb24a406391', '0xe3aac29001c769fafcef0df072ca396e310ed13b')
                THEN (platformManagementFee_ + platformServiceFee_) / POW(10, 18) * p.price
            ELSE (platformManagementFee_ + platformServiceFee_) / POW(10, 6)
        END AS platform_fees_usd,
        CASE 
            WHEN contract_address in ('0x373bdcf21f6a939713d5de94096ffdb24a406391', '0xe3aac29001c769fafcef0df072ca396e310ed13b')
                THEN (platformManagementFee_ + platformServiceFee_) / POW(10, 18)
            ELSE (platformManagementFee_ + platformServiceFee_) / POW(10, 6)
        END AS platform_fees_native,
        CASE
            WHEN contract_address in ('0x373bdcf21f6a939713d5de94096ffdb24a406391', '0xe3aac29001c769fafcef0df072ca396e310ed13b')
                THEN (delegateManagementFee_ + delegateServiceFee_) / POW(10, 18) * p.price
            ELSE (delegateManagementFee_ + delegateServiceFee_) / POW(10, 6)
        END AS delegate_fees_usd,
        CASE
            WHEN contract_address in ('0x373bdcf21f6a939713d5de94096ffdb24a406391', '0xe3aac29001c769fafcef0df072ca396e310ed13b')
                THEN (delegateManagementFee_ + delegateServiceFee_) / POW(10, 18)
            ELSE (delegateManagementFee_ + delegateServiceFee_) / POW(10, 6)
        END AS delegate_fees_native
    from {{ ref('fact_maple_v2_OpenTermLoanManager_ClaimedFundsDistributed') }} f
    left join eth_prices p on p.hour = date_trunc('hour', f.block_timestamp)
    LEFT JOIN {{ ref('dim_maple_pools') }} pools ON f.contract_address = pools.open_term_loan_manager 
)
, agg as(
    SELECT
        *
    FROM ftlm_interest
    UNION ALL
    SELECT
        *
    FROM otlm_interest
)
SELECT * FROM agg 
WHERE pool_name is not null