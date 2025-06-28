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
            sum(case when index = 0 then tx_fee + COALESCE(jito_tips, 0) else 0 end) rev,
            sum(case when index = 0 then gas_usd + COALESCE(jito_tips_usd, 0) else 0 end) rev_usd,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau,
            max(category) category
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        where
            not equal_null(category, 'EOA')
            {% if is_incremental() %}
                and date >= (select dateadd('day', CASE WHEN DAYOFWEEK(CURRENT_DATE) = 6 THEN -90 ELSE -30 END, max(date)) from {{ this }})
            {% endif %}
        group by date, contract_address
    ),
    monthly_users as (
        select
            coalesce(token_address, program_id) as contract_address,
            date_trunc('month', raw_date) as month_date,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) mau
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        where
            not equal_null(category, 'EOA')
            {% if is_incremental() %}
                and raw_date >= (select dateadd('day', CASE WHEN DAYOFWEEK(CURRENT_DATE) = 6 THEN -120 ELSE -60 END, max(date)) from {{ this }})
            {% endif %}
        group by month_date, contract_address
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
        WHEN dau = 0 OR dau IS NULL THEN NULL
        ELSE gas / dau
    END AS avg_gas_per_address,
    gas_usd,
    CASE 
        WHEN dau = 0 OR dau IS NULL THEN NULL
        ELSE gas_usd / dau
    END AS avg_gas_usd_per_address,
    rev,
    rev_usd,
    txns,
    dau,
    mau,
    null AS real_users
from contract_data
left join
    monthly_users
    on contract_data.contract_address = monthly_users.contract_address
    and date_trunc('month', contract_data.date) = monthly_users.month_date
