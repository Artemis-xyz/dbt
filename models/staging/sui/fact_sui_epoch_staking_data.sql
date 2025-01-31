{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_sui_epoch_data") }}
        
    ),
    latest_data as (
        select parse_json(source_json):data:records as data
        from {{ source("PROD_LANDING", "raw_sui_epoch_data") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    f.value:date::date as date,
    f.value:epoch::number as epoch,
    f.value:leftover_storage_fund_inflow::number as leftover_storage_fund_inflow,
    f.value:protocol_version::number as protocol_version,
    f.value:reference_gas_price::number as reference_gas_price,
    f.value:stake_subsidy_amount::number as stake_subsidy_amount, -- we use this one for mints
    f.value:storage_charge::number as storage_charge,
    f.value:storage_fund_balance::number as storage_fund_balance,
    f.value:storage_fund_reinvestment::number as storage_fund_reinvestment,
    f.value:storage_rebate::number as storage_rebate,
    f.value:total_gas_fees::number as total_gas_fees,
    f.value:total_stake::number as total_stake,
    f.value:total_stake_rewards_distributed::number as total_stake_rewards_distributed -- = stake_subsidy_amount + gas_fees
from latest_data, lateral flatten(input => data) f