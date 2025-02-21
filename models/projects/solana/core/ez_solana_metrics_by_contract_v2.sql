{{
    config(
        materialized="incremental",
        unique_key=["contract_address", "date"],
        snowflake_warehouse="SOLANA_XLG",
        database="solana",
        schema="core",
        alias="ez_metrics_by_contract_v2",
    )
}}
with
    contract_data as (
        select
            coalesce(token_address, program_id) as contract_address,
            raw_date as date,
            max(coalesce(token_name, name)) name,
            max(app) as namespace,
            max(friendly_name) as friendly_name,
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then gas_usd else 0 end) gas_usd,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau,
            max(category) category
        from {{ ref("ez_solana_transactions_v2") }}, lateral flatten(input => signers)
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
    'solana' as chain,
    name,
    namespace,
    friendly_name,
    case when category = 'Tokens' then 'Token' else category end as category,
    gas,
    gas_usd,
    txns,
    dau
from contract_data
