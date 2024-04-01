{{ config(materialized="table", snowflake_warehouse="DAILY_BAM") }}

with
    contract_data as (
        select
            contract_address,
            raw_date as date,
            max(name) name,
            max(app) as namespace,
            max(friendly_name) as friendly_name,
            sum(tx_fee) total_gas,
            sum(gas_usd) total_gas_usd,
            count(*) transactions,
            count(distinct from_address) dau,
            max(category) category
        from {{ ref("fact_near_transactions_gold") }}
        where
            not equal_null(category, 'EOA')
            {% if is_incremental() %}
                and date >= (select dateadd('day', -7, max(date)) from {{ this }})
            {% endif %}
        group by date, contract_address
    )
select
    contract_data.contract_address,
    contract_data.date,
    name,
    null as symbol,
    namespace,
    friendly_name,
    category,
    total_gas,
    total_gas_usd,
    transactions,
    dau,
    null as token_transfer_usd,
    null as token_transfer,
    null as avg_token_price
from contract_data
