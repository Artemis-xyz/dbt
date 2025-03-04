{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="SEI_LG",
    )
}}
with
    evm_txs AS (
        select 
            distinct tx_id from sei_flipside.core.fact_msg_attributes 
        where 
            msg_type ='message' and attribute_key = 'action' and ATTRIBUTE_VALUE = '/seiprotocol.seichain.evm.MsgEVMTransaction'
        {% if is_incremental() %}
            AND inserted_timestamp >= (select dateadd('day', -5, max(inserted_timestamp)) from {{ this }})
        {% endif %}
    ),
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
        where chain = 'sei'
    ),
    prices as (
        select date as price_date, shifted_token_price_usd as price
        from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
        where coingecko_id = 'sei-network'
        union
        select dateadd('day', -1, date) as price_date, token_current_price as price
        from pc_dbt_db.prod.fact_coingecko_token_realtime_data
        where token_id = 'sei-network'
    ),
    collapsed_prices as (
        select price_date, max(price) as price from prices group by price_date
    ),
    msg_atts_base AS (
        SELECT
            max(block_timestamp) as block_timestamp,
            max(date(block_timestamp)) as raw_date,
            max(tx_succeeded) as tx_succeeded,
            tx_id,
            max (case when attribute_key in ('_contract_address', 'contract_address') then attribute_value else null end) as contract_address_1,
            max (
                case 
                    when msg_type = 'transfer' then 'sei_token_transfer'
                    when msg_type = 'ibc_transfer' then 'sei_ibc_transfer'
                    when msg_type = 'message' AND attribute_key = 'action' AND attribute_value = '/seiprotocol.seichain.oracle.MsgAggregateExchangeRateVote'
                        then 'sei_oracle_votes'
                    when msg_type = 'instantiate' AND attribute_key = 'contract_address' then 'sei_create_contract'
                    when msg_type = 'message' AND attribute_key = 'action' AND attribute_value = '/cosmos.gov.v1beta1.MsgVote' then 'sei_governance_votes'
                    when msg_type = 'message' and attribute_key = 'module' and attribute_key = 'staking' then 'sei_staking'
                    when msg_type = 'message' and attribute_key = 'module' and attribute_value = 'oracle' then 'sei_oracle'
                    when msg_type = 'withdraw_rewards' then 'sei_staking_withdraw'
                    when msg_type = 'aggregate_vote' then 'sei_voting'
                    when msg_type = 'coin_spent' then 'sei_staking_coin_spent'
                    when msg_type in ('delegate','redelegate','unbond','create_validator') then 'sei_staking'
                    else null
                end
            ) as contract_address_2,
            max(inserted_timestamp) as inserted_timestamp
        FROM
            sei_flipside.core.fact_msg_attributes
            {% if is_incremental() %}
            WHERE inserted_timestamp >= (select dateadd('day', -5, max(inserted_timestamp)) from {{ this }})
            {% endif %}
        GROUP BY tx_id
    ),
    transaction_contract_data as (
        SELECT
            tx_id as tx_hash
            , block_timestamp
            , raw_date
            , tx_succeeded
            , coalesce(contract_address_1, contract_address_2) as contract_address
            , inserted_timestamp
        FROM
            msg_atts_base
    ),
    sei_transactions as (
        SELECT 
            t1.tx_hash
            , t1.block_timestamp
            , t1.raw_date
            , t1.tx_succeeded as success
            , t1.contract_address
            , t3.name
            , t3.app
            , t3.friendly_name
            , t3.sub_category
            , t3.category
            , t2.tx_from as signer
            , (split(t2.fee, 'usei')[0] / pow(10, 6)) as tx_fee
            , (split(t2.fee, 'usei')[0] / pow(10, 6)) * t4.price as gas_usd
            , t1.inserted_timestamp
        FROM 
            transaction_contract_data as t1
        LEFT JOIN 
            sei_flipside.core.fact_transactions as t2
            ON t1.tx_hash = t2.tx_id
        LEFT JOIN 
            new_contracts as t3
            ON t1.contract_address = t3.address
        LEFT JOIN 
            prices as t4
            ON t1.raw_date = t4.price_date
        WHERE
            t1.block_timestamp < date(sysdate())
            {% if is_incremental() %}
            AND 
            t2.inserted_timestamp >= (select dateadd('day', -5, max(inserted_timestamp)) from {{ this }})
            {% endif %}
            AND tx_id NOT IN (
                SELECT tx_id
                FROM evm_txs e 
            )
    )
    SELECT
        tx_hash
        , max(success) as success
        , max(block_timestamp) as block_timestamp
        , max(raw_date) as raw_date
        , max(signer) as signer
        , max(tx_fee) as tx_fee
        , max(gas_usd) as gas_usd
        , 'sei' as chain
        , max(contract_address) as contract_address
        , max(name) as name
        , max(app) as app
        , max(friendly_name) as friendly_name
        , max(sub_category) as sub_category
        , max(inserted_timestamp) as inserted_timestamp
        , max(category) as category
        , null as user_type
        , null as address_life_span
        , null as cur_total_txns
        , null as cur_distinct_to_address_count
        , null as probability
        , null as engagement_type
        , null as balance_usd
        , null as native_token_balance
        , null as stablecoin_balance
    FROM sei_transactions
    group by tx_hash
