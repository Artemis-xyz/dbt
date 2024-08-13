{{ config(
    materialized="incremental",
    unique_key="date",
) }}

with flatten_ton_transaction as (
    select
        NOT ARRAY_CONTAINS(0, ARRAY_UNIQUE_AGG(success)) as success
        , trace_id
        , max(block_timestamp::date) as date
        , min_by(transaction_account_interfaces, lt) as interfaces
        , min_by(transaction_account, lt) as first_account
        , min_by(transaction_account_workchain, lt) as workchain
        , sum(total_fees) as transaction_fees
    from {{ ref('fact_ton_transactions') }}
    {% if is_incremental() %}
        where block_timestamp::date > (select dateadd('day', -3, max(date)) from {{ this }})
    {% endif %}
    group by trace_id
), txns as (
    SELECT
        date
        , count(*) as txns
        , (sum(transaction_fees) / POWER(10, 9)) / count(*) as avg_txn_fee_native
    from flatten_ton_transaction
    where success and workchain <> -1
    group by date
), fees as (
    SELECT
        date
        , sum(transaction_fees) / POWER(10, 9) as fees_native
    FROM flatten_ton_transaction
    GROUP by date
),
dau as (
    select
        date
        , count(distinct first_account) as dau
    from flatten_ton_transaction
    where success and interfaces[1] like 'wallet_v%'
    group by date
)
SELECT
    coalesce(fees.date, txns.date, dau.date) as date
    , dau
    , fees_native
    , txns
    , avg_txn_fee_native
FROM fees left join txns on fees.date = txns.date
left join dau on fees.date = dau.date
