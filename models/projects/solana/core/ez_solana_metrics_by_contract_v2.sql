-- depends_on {{ ref('fact_solana_transactions_v2') }}
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
            sum(case when index = 0 then tx_fee + jito_tips else 0 end) rev,
            sum(case when index = 0 then gas_usd + jito_tips_usd else 0 end) rev_usd,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau,
            max(category) category
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        where
            not equal_null(category, 'EOA')
            {% if is_incremental() %}
                and date >= (select dateadd('day', CASE WHEN DAYOFWEEK(CURRENT_DATE) = 7 THEN -90 ELSE -30 END, max(date)) from {{ this }})
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
    CASE 
        WHEN gas = 0 OR gas IS NULL THEN NULL
        ELSE dau / gas
    END AS avg_gas_per_address,
    gas_usd,
    CASE 
        WHEN gas_usd = 0 OR gas_usd IS NULL THEN NULL
        ELSE dau / gas_usd
    END AS avg_gas_usd_per_address,
    rev,
    rev_usd,
    txns,
    dau,
    null AS real_users
from contract_data
