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
            count(*) txns,
            count(distinct sender) dau,
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
    name,
    app,
    friendly_name,
    category,
    gas,
    gas_usd,
    txns,
    dau
from contract_data