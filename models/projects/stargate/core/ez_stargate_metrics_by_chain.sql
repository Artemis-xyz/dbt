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
    date,
    'stargate' as chain,
    total_transactions as txns,
    total_volume as bridge_volume,
    active_addresses as dau,
    new_addresses,
    returning_addresses,
    avg_transaction_size as avg_txn_size
from 
{{ ref('fact_stargate_metrics_by_chain') }}