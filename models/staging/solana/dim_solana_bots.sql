{{ config(materialized="table", snowflake_warehouse="BAM_TRANSACTION_MD") }}

with
    sleep_daily_count as (
        select
            count_if(user_type = 'LESS THAN 7 HOURS') as no_sleep_count,
            from_address
        from {{ ref('fact_solana_daily_sleep') }}
        group by from_address
    ),
    from_address_agg_metrics as (
        -- Use EZ_SIGNERS from flipside instead of solana_flipside.core.fact_transactions directly
        SELECT
            signer AS from_address,
            last_tx_date::TIMESTAMP AS latest_transaction_timestamp,
            first_tx_date::TIMESTAMP AS first_transaction_timestamp,
            num_txs AS cur_total_txns,
            ARRAY_SIZE(programs_used) AS cur_distinct_to_address_count
        from solana_flipside.core.ez_signers
    )
select
    agg_metrics.from_address,
    no_sleep_count,
    first_transaction_timestamp,
    case
        when no_sleep_count >= 5 then 'LOW_SLEEP' else 'HIGH_SLEEP'
    end as user_type,
    datediff(
        'day', first_transaction_timestamp, latest_transaction_timestamp
    ) as address_life_span,
    cur_total_txns,
    cur_distinct_to_address_count
from from_address_agg_metrics as agg_metrics
left join
    sleep_daily_count on agg_metrics.from_address = sleep_daily_count.from_address
