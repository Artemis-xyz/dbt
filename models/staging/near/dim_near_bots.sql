{{ config(materialized="table", snowflake_warehouse="BAM_TRANSACTION_SM") }}
with
    sleep_daily_count as (
        select count_if(user_type = 'LESS THAN 7 HOURS') as no_sleep_count, from_address
        from {{ ref("fact_near_daily_sleep") }}
        group by from_address
    ),
    tx_signer_agg_metrics as (
        select
            tx_signer as from_address,
            max(block_timestamp) as latest_transaction_timestamp,
            min(block_timestamp) as first_transaction_timestamp,
            count(*) as cur_total_txns,
            count(distinct tx_receiver) as cur_distinct_to_address_count
        from near_flipside.core.fact_transactions
        group by tx_signer
    )
select
    agg_metrics.from_address,
    case when no_sleep_count >= 5 then 'LOW_SLEEP' else 'HIGH_SLEEP' end as user_type,
    datediff(
        'day', first_transaction_timestamp, latest_transaction_timestamp
    ) as address_life_span,
    cur_total_txns,
    cur_distinct_to_address_count
from tx_signer_agg_metrics as agg_metrics
left join sleep_daily_count on agg_metrics.from_address = sleep_daily_count.from_address
