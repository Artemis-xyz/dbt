{{ config(materialized="table", snowflake_warehouse="DAILY_BAM") }}

with
    contract_data as (
        select
            contract_address,
            date_trunc('day', block_timestamp) date,
            max(name) name,
            max(app) as namespace,
            max(friendly_name) friendly_name,
            sum(tx_fee) total_gas,
            sum(gas_usd) total_gas_usd,
            count(*) transactions,
            count(distinct from_address) dau,
            max(category) category
        from {{ ref("fact_avalanche_transactions_gold") }}
        where not equal_null(category, 'EOA')
        group by date, contract_address
    ),
    transfer_volume as (
        select
            sum(amount_usd) token_transfer_usd,
            sum(amount) token_transfer,
            avg(token_price) avg_token_price,
            date_trunc('day', block_timestamp) as date,
            lower(contract_address) contract_address,
            max(symbol) as symbol
        from avalanche_flipside.core.ez_token_transfers
        where amount_usd < 1000000000
        group by date, contract_address
    )
select
    contract_data.contract_address,
    contract_data.date,
    name,
    symbol,
    namespace,
    friendly_name,
    category,
    total_gas,
    total_gas_usd,
    transactions,
    dau,
    token_transfer_usd,
    token_transfer,
    avg_token_price
from contract_data
left join
    transfer_volume
    on contract_data.contract_address = transfer_volume.contract_address
    and contract_data.date = transfer_volume.date
