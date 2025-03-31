{{
    config(
        materialized="table",
        snowflake_warehouse="FILECOIN",
    )
}}

with max_extraction as (
    select
        max(extraction_date) as max_date
    from
        landing_database.PROD_LANDING.raw_filecoin_revenue
),
latest_data as (
    select
        parse_json(source_json) as data
    from
        landing_database.PROD_LANDING.raw_filecoin_revenue
    where
        extraction_date = (
            select
                max_date
            from
                max_extraction
        )
),
flattened_data as (
    select
        f.value:stat_date::date as date,
        f.value:base_fee_burn::number as base_fee_burn_native,
        f.value:miner_tip::number as miner_tip_native,
        f.value:overestimation_burn::number as overestimation_burn_native,
        f.value:penalty_fee_burn::number as penalty_fee_burn_native,
        f.value:precommit_batch_fee_burn::number as precommit_batch_fee_burn_native,
        f.value:provecommit_batch_fee_burn::number as provecommit_batch_fee_burn_native
    from
        latest_data,
        lateral flatten(input => data) as f
)
select
    date,
    base_fee_burn_native,
    miner_tip_native,
    overestimation_burn_native,
    penalty_fee_burn_native,
    precommit_batch_fee_burn_native,
    provecommit_batch_fee_burn_native,
    base_fee_burn_native + miner_tip_native + overestimation_burn_native + penalty_fee_burn_native + precommit_batch_fee_burn_native + provecommit_batch_fee_burn_native as total_burn_native
from
    flattened_data
where
    date < to_date(sysdate())
order by
    date desc