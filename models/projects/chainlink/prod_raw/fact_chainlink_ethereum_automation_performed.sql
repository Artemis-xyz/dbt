{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_automation_performed",
    )
}}

select
    'ethereum' as chain
    , max(operator_name) as operator_name
    , max(coalesce(keeper_address, automation_logs.tx_from)) as keeper_address
    , max(automation_logs.block_timestamp) as evt_block_time
    , max(coalesce(decoded_log:"payment"::number, decoded_log:"totalPayment"::number) / 1e18) as token_value
from
  {{ ref('fact_chainlink_ethereum_automation_upkeep_performed_logs') }} automation_logs
  left join {{ ref('dim_chainlink_ethereum_automation_meta') }} automation_meta ON automation_meta.keeper_address = automation_logs.tx_from
group by
  tx_hash,
  event_index,
  tx_from