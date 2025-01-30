{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE",
        database="stargate",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

select 
    dst_chain,
    total_transactions,
    total_volume,
    active_addresses,
    new_addresses,
    returning_addresses,
    avg_transaction_size
from 
{{ ref('fact_stargate_metrics_by_chain') }}