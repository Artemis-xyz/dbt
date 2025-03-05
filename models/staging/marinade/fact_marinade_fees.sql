{{ config(
    materialized="table",
    snowflake_warehouse="MEDIUM"
) }}

with claim_events as (
    select
        f.value:name::STRING as account_name,
        f.value:pubkey::STRING as pubkey,
        di.block_timestamp,
        di.tx_id,
        di.program_id,
        di.event_type,
        di.decoded_instruction
    from solana_flipside.core.fact_decoded_instructions di,
        lateral flatten(input => parse_json(decoded_instruction):accounts) f
    where di.program_id = 'vBoNdEvzMrSai7is21XgVYik65mqtaKXuSdMBJ1xkW4'
      and di.event_type ilike 'claimSettlement%' 
      and f.value:name::STRING ilike '%stake%'
      {% if is_incremental() %}
        and date(di.block_timestamp) >= (select dateadd('day', -3, max(date)) from {{ this }})
      {% endif %}
),
unique_tx as (
    select 
        ce.tx_id,
        ft.block_timestamp,
        ft.pre_balances,
        ft.post_balances,
        row_number() over (partition by ce.tx_id order by ft.block_timestamp) as row_num
    from claim_events ce
    left join solana_flipside.gov.fact_stake_accounts fsa on fsa.stake_pubkey = ce.pubkey
    left join solana_flipside.core.fact_transactions ft on ft.tx_id = ce.tx_id
    where fsa.authorized_staker = '89SrbjbuNyqSqAALKBsKBqMSh463eLvzS4iVWCeArBgB'
    {% if is_incremental() %}
        and date(ft.block_timestamp) >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% endif %}
),
date_spine as (
    select
        date
    from
        pc_dbt_db.prod.dim_date_spine
    where date between (select min(date(unique_tx.block_timestamp)) from unique_tx) 
                  and to_date(sysdate())
),
v2_fees as (
    select 
        date(unique_tx.block_timestamp) as date,
        sum(abs(cast(unique_tx.pre_balances[1] as numeric) / 1e9 - cast(unique_tx.post_balances[1] as numeric) / 1e9)) as fees_native
    from unique_tx
    where row_num = 1
    group by date(unique_tx.block_timestamp)
)
select 
    ds.date,
    coalesce(v2.fees_native, 0) as fees_native
from date_spine ds
left join v2_fees v2 on ds.date = v2.date