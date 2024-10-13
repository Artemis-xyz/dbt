with eth_prices as (
    select * from {{ source('ETHEREUM_FLIPSIDE', 'ez_prices_hourly') }}
    where is_native = True
)
, ftlm_interest as (
    select
        block_timestamp,
        tx_hash,
        contract_address,
        CASE WHEN
            contract_address = '0x373bdcf21f6a939713d5de94096ffdb24a406391'
            THEN netinterest_ / POW(10,18) * p.price
        ELSE netinterest_ / POW(10, 6)
        END AS net_interest
    from {{ ref('fact_maple_v2_LoanManager_FundsDistributed') }} f
    left join eth_prices p on p.hour = date_trunc('hour', f.block_timestamp)
)

-- OTLM (448 rows)
, otlm_interest as (
    select
        block_timestamp,
        contract_address,
        tx_hash,
        CASE WHEN 
            contract_address in ('0x373bdcf21f6a939713d5de94096ffdb24a406391', '0xe3aac29001c769fafcef0df072ca396e310ed13b')
            THEN netinterest_ / POW(10,18) * p.price
        ELSE netinterest_ / POW(10, 6)
        END AS net_interest
    from {{ ref('fact_maple_v2_OpenTermLoanManager_ClaimedFundsDistributed') }} f
    left join eth_prices p on p.hour = date_trunc('hour', f.block_timestamp)
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
SELECT
    block_timestamp,
    contract_address,
    p.asset,
    tx_hash,
    net_interest
FROM agg
LEFT JOIN {{ ref('dim_maple_pools') }} p ON agg.contract_address = p.loan_manager