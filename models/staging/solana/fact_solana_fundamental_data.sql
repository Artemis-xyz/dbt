{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="SOLANA_XLG",
        on_schema_change='append_new_columns'
    )
}}

with agg_data as (
    select
        raw_date,
        max(chain) as chain,
        sum(case when index = 0 then tx_fee else 0 end) gas,
        sum(case when index = 0 then gas_usd else 0 end) gas_usd,
        median(case when index = 0 then gas_usd end) as median_txn_fee,
        sum(
            case when index = 0 then (array_size(signers) * (5000 / 1e9)) else 0 end
        ) as base_fee_native,
        count_if(index = 0) as txns,
        count(distinct(case when succeeded = 'TRUE' then value else null end)) dau
    from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
    {% if is_incremental() %}
        where raw_date > (select dateadd('day', -3, max(date)) from {{ this }})
    {% endif %}
    group by raw_date
)
, price as ({{ get_coingecko_metrics("solana") }})
, min_date as (
    select min(raw_date) as start_date, value as signer
    from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
    where succeeded = 'TRUE'
    group by signer
)
, new_users as (
    select count(distinct signer) as new_users, start_date
    from min_date
    group by start_date
)

{% if not is_incremental() %}
    , unrefreshed_data as (
        select
            date_trunc('day', block_timestamp) as date,
            sum(case when index = 0 then fee / pow(10, 9) else 0 end) gas,
            median(case when index = 0 then fee / pow(10, 9) end) as median_txn_fee_native,
            sum(
                case
                    when index = 0 then (array_size(signers) * (5000 / 1e9)) else 0
                end
            ) as base_fee_native,
            count_if(index = 0) as txns,
            count(
                distinct(case when succeeded = 'TRUE' then value else null end)
            ) dau,
            null as returning_users,
            null as new_users
        from
            solana_flipside.core.fact_transactions,
            lateral flatten(input => signers)
        where
            date_trunc('day', block_timestamp)
            < (select min(raw_date) from {{ ref('fact_solana_transactions_v2') }})
        group by date
    )
    , unrefreshed_data_with_price as (
        select
            unrefreshed_data.date,
            gas,
            gas * price as gas_usd,
            base_fee_native,
            txns,
            dau,
            returning_users,
            new_users,
            median_txn_fee_native * price as median_txn_fee
        from unrefreshed_data
        left join price on unrefreshed_data.date = price.date
    )
{% endif %}
, voting_fees as (
    select
        date_trunc('day', block_timestamp) as date,
        sum(num_votes * 5000) / pow(10, 9) as vote_tx_fee_native
    from solana_flipside.gov.fact_votes_agg_block
    {% if is_incremental() %}
        where
            date_trunc('day', block_timestamp)
            > (select dateadd('day', -3, max(date)) from {{ this }})
    {% endif %}
    group by date
)


select
    agg_data.raw_date as date,
    gas,
    gas_usd,
    median_txn_fee,
    base_fee_native,
    txns,
    dau,
    (dau - new_users) as returning_users,
    new_users,
    vote_tx_fee_native
from agg_data
left join new_users on date = new_users.start_date
left join voting_fees on agg_data.raw_date = voting_fees.date
{% if not is_incremental() %}
    union
    select
        date,
        gas,
        gas_usd,
        median_txn_fee,
        base_fee_native,
        txns,
        dau,
        returning_users,
        new_users,
        NULL as vote_tx_fee_native
    from unrefreshed_data_with_price
{% endif %}
