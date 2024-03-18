{{ config(materialized="table", snowflake_warehouse="BAM_TRANSACTION_XLG") }}

with
    contract_data as (
        select
            coalesce(token_address, program_id) as contract_address,
            raw_date as date,
            max(coalesce(token_name, name)) name,
            max(app) as namespace,
            max(friendly_name) as friendly_name,
            sum(case when index = 0 then tx_fee else 0 end) total_gas,
            sum(case when index = 0 then gas_usd else 0 end) total_gas_usd,
            count_if(index = 0 and succeeded = 'TRUE') as transactions,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau,
            max(category) category
        from
            {{ ref("fact_solana_transactions_gold") }},
            lateral flatten(input => signers)
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
    case when category = 'Tokens' then 'Token' else category end as category,
    total_gas,
    total_gas_usd,
    transactions,
    dau,
    null as token_transfer_usd,
    null as token_transfer,
    null as avg_token_price
from contract_data
