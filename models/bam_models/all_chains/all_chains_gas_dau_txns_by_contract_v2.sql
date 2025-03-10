{{ config(materialized="table", snowflake_warehouse="DAILY_BAM") }}

select
    LOWER(contract_address) AS contract_address,
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
    'arbitrum' as chain
from {{ ref("ez_arbitrum_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
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
    'avalanche' as chain
from {{ ref("ez_avalanche_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
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
    'base' as chain
from {{ ref("ez_base_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
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
    'bsc' as chain
from {{ ref("ez_bsc_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
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
    'ethereum' as chain
from {{ ref("ez_ethereum_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
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
    'optimism' as chain
from {{ ref("ez_optimism_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
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
    'polygon' as chain
from {{ ref("ez_polygon_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
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
    'near' as chain
from {{ ref("ez_near_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
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
    'sei' as chain
from {{ ref("ez_sei_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
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
from {{ ref("ez_tron_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
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
    'mantle' as chain
from {{ ref("ez_mantle_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
    date,
    name::string as name,
    null as symbol,
    app::string as namespace,
    friendly_name::string as friendly_name,
    category::string as category,
    gas as total_gas,
    gas_usd as total_gas_usd,
    txns as transactions,
    dau,
    null as token_transfer_usd,
    null as token_transfer,
    null as avg_token_price,
    'sui' as chain
from {{ ref("ez_sui_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
    date,
    name::string as name,
    null as symbol,
    app::string as namespace,
    friendly_name::string as friendly_name,
    category::string as category,
    gas as total_gas,
    gas_usd as total_gas_usd,
    txns as transactions,
    dau,
    null as token_transfer_usd,
    null as token_transfer,
    null as avg_token_price,
    'injective' as chain
from {{ ref("ez_injective_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
    date,
    name::string as name,
    null as symbol,
    app::string as namespace,
    friendly_name::string as friendly_name,
    category::string as category,
    gas as total_gas,
    gas_usd as total_gas_usd,
    txns as transactions,
    dau,
    null as token_transfer_usd,
    null as token_transfer,
    null as avg_token_price,
    'stellar' as chain
from {{ ref("ez_stellar_metrics_by_contract_v2") }}
union
select
    LOWER(contract_address) AS contract_address,
    date,
    name,
    null as symbol,
    namespace,
    friendly_name,
    category,
    gas as total_gas,
    gas_usd as total_gas_usd,
    txns as transactions,
    dau,
    null as token_transfer_usd,
    null as token_transfer,
    null as avg_token_price,
    'solana' as chain
from {{ ref("ez_solana_metrics_by_contract_v2") }}
