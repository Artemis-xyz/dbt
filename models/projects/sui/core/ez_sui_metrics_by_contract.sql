{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="SUI_MD",
        database="sui",
        schema="core",
        alias="ez_metrics_by_contract",
    )
}}
with
    dau_txns as (
        select
            package as contract_address,
            raw_date as date,
            count(*) txns,
            count(distinct sender) dau
        from {{ ref("ez_sui_transactions") }}
        where status = 'success'
        group by raw_date, contract_address
    ),
    contract_data as (
        select
            package as contract_address,
            raw_date as date,
            max(name) name,
            max(chain) as chain,
            max(app) as app,
            max(friendly_name) as friendly_name,
            sum(tx_fee) gas,
            sum(gas_usd) gas_usd,
            max(category) category
        from {{ ref("ez_sui_transactions") }}
        where
            not equal_null(category, 'EOA')
            {% if is_incremental() %}
                and date >= (select dateadd('day', -7, max(date)) from {{ this }})
            {% endif %}
        group by date, contract_address
    )
select
    contract_data.date,
    contract_data.contract_address,
    chain,
    contract_data.name,
    contract_data.app,
    contract_data.friendly_name,
    contract_data.category,
    gas,
    gas_usd,
    txns,
    dau
from contract_data
join dau_txns
    on contract_data.date = dau_txns.date
    and contract_data.contract_address = dau_txns.contract_address
