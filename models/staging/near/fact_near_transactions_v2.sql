{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BAM_TRANSACTION_XLG",
    )
}}

with
    new_contracts as (
        select distinct
            address,
            contract.name,
            contract.chain,
            contract.artemis_category_id as category,
            contract.artemis_sub_category_id as sub_category,
            contract.artemis_application_id as app,
            contract.friendly_name
        from {{ ref("dim_all_addresses_labeled_gold") }} as contract
        where chain = 'near'
    ),
    prices as (
        select date as price_date, shifted_token_price_usd as price
        from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
        where coingecko_id = 'near'
        union
        select dateadd('day', -1, date) as price_date, token_current_price as price
        from pc_dbt_db.prod.fact_coingecko_token_realtime_data
        where token_id = 'near'
    ),
    collapsed_prices as (
        select price_date, max(price) as price from prices group by price_date
    ),
    near_transactions as (
        select 
            tx_hash
            , case when tx:"actions"[0]:"Delegate" is not null then tx_signer else tx_receiver end as contract_address
            , block_timestamp
            , tx_succeeded
            , date_trunc('day', block_timestamp) raw_date
            , case when tx:"actions"[0]:"Delegate" is not null then tx_receiver else tx_signer end as from_address
            , (transaction_fee / pow(10, 24)) as tx_fee
            , ((transaction_fee / pow(10, 24)) * price) gas_usd
            , 'near' as chain
            , case when (tx:"actions"[0]:"Transfer":"deposit" / pow(10, 24) > .02) then 'EOA' end as category
        from near_flipside.core.fact_transactions as t
        left join collapsed_prices on raw_date = collapsed_prices.price_date
        where raw_date < to_date(sysdate()) and inserted_timestamp < to_date(sysdate())
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run 
            and block_timestamp
            >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
    )
select
    tx_hash,
    new_contracts.address as contract_address,
    block_timestamp,
    raw_date,
    t.from_address,
    tx_fee,
    gas_usd,
    t.chain,
    new_contracts.name,
    new_contracts.app,
    new_contracts.friendly_name,
    new_contracts.sub_category,
    case
        when t.category is not null
        then t.category
        when new_contracts.category is not null
        then new_contracts.category
        else null
    end as category,
    bots.user_type,
    bots.address_life_span,
    bots.cur_total_txns,
    bots.cur_distinct_to_address_count,
    tx_succeeded
from near_transactions as t
left join new_contracts on lower(t.contract_address) = lower(new_contracts.address)
left join {{ ref("dim_near_bots") }} as bots on t.from_address = bots.from_address
