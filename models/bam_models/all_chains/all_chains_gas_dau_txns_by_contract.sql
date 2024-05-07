{{ config(materialized="table", snowflake_warehouse="DAILY_BAM") }}

select *, 'arbitrum' as chain
from {{ ref("arb_gas_dau_txns_by_contract") }}
union
select *, 'avalanche' as chain
from {{ ref("avax_gas_dau_txns_by_contract") }}
union
select *, 'base' as chain
from {{ ref("agg_daily_base_contracts_fundamental_usage") }}
union
select *, 'bsc' as chain
from {{ ref("bsc_gas_dau_txns_by_contract") }}
union
select *, 'near' as chain
from {{ ref("agg_daily_near_contracts_fundamental_usage") }}
union
select *, 'ethereum' as chain
from {{ ref("eth_gas_dau_txns_by_contract") }}
union
select *, 'polygon' as chain
from {{ ref("polygon_gas_dau_txns_by_contract") }}
union
select *, 'optimism' as chain
from {{ ref("opt_gas_dau_txns_by_contract") }}
union
select *, 'solana' as chain
from {{ ref("agg_daily_solana_contracts_fundamental_usage") }}
union
select
    contract_address,
    date,
    name,
    null as symbol,
    app as namespace,
    friendly_name,
    category,
    gas as total_gas,
    gas_usd as total_gas_usd,
    txns as transactions,
    dau,
    null as token_transfer_usd,
    null as token_transfer,
    null as avg_token_price,
    'tron' as chain
from {{ ref("ez_tron_metrics_by_contract") }}
union
select
    contract_address,
    date,
    name,
    null as symbol,
    app as namespace,
    friendly_name,
    category,
    gas as total_gas,
    gas_usd as total_gas_usd,
    txns as transactions,
    dau,
    null as token_transfer_usd,
    null as token_transfer,
    null as avg_token_price,
    'sui' as chain
from {{ ref("ez_sui_metrics_by_contract") }}
