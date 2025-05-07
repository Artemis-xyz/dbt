{{
    config(
        materialized="table",
    )
}}

with
    prices as ({{get_coingecko_price_with_latest("near")}}),
    blob_transactions as (
        select 
            tx_hash,
            block_timestamp::date as date,
            receipt_signer_id as submitter,
            len(action_data:args::VARCHAR) * 6 / 8 as bytes
        from near_flipside.core.ez_actions
        --Will need to scale out as more l2s use nearDA, right now just one tho
        where receipt_signer_id = 'vsl-submitter.near'
            and action_data:method_name::VARCHAR = 'submit'
    ),
    blob_gas_fees as (
        select 
            blob_transactions.date,
            sum(transaction_fee / 1e24) as blob_fees_native,
            sum(bytes) / pow(1024, 2) as blob_size_mib,
            count(distinct submitter) as submitters
        from near_flipside.core.fact_transactions
        inner join blob_transactions using(tx_hash)
        group by blob_transactions.date
    )
select 
    blob_gas_fees.date,
    blob_fees_native,
    blob_fees_native * price as blob_fees,
    blob_size_mib,
    blob_size_mib / 86400 as avg_mib_per_second,
    blob_fees_native / (blob_size_mib) as avg_cost_per_mib_native,
    blob_fees / (blob_size_mib) as avg_cost_per_mib,
    submitters
from blob_gas_fees
left join prices using(date)
where blob_gas_fees.date < to_date(sysdate())

