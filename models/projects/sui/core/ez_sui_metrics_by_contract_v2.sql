{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
        database="sui",
        schema="core",
        alias="ez_metrics_by_contract_v2",
    )
}}
-- Based on the `get_fundamental_data_for_chain_by_contractn` macro in the primary
-- repo
with
    contract_data as (
        select
            package as contract_address,
            raw_date as date,
            max(name) name,
            max(chain) as chain,
            max(app) as app,
            max(friendly_name) as friendly_name,
            ROUND(sum(tx_fee), 3) gas,
            sum(gas_usd) gas_usd,
            count(*) txns,
            count(distinct from_address) dau,
            max(category) category
        from {{ source("PROD_LANDING", "fact_sui_transactions_silver_bigquery_v2") }}
        group by date, contract_address
    )
select
    TO_TIMESTAMP_NTZ(contract_data.date) as date,
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
where contract_data.date < current_date()