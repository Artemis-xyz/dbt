{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="SOLANA",
    )
}}
with
    new_contracts as (
        select distinct
            address,
            contract.name,
            contract.category,
            contract.sub_category,
            contract.app,
            contract.friendly_name
        from {{ ref("dim_contracts_gold") }} as contract
        where chain = 'sei'
    ),
    prices as (
        select date as price_date, shifted_token_price_usd as price
        from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
        where coingecko_id = 'sei'
        union
        select dateadd('day', -1, date) as price_date, token_current_price as price
        from pc_dbt_db.prod.fact_coingecko_token_realtime_data
        where token_id = 'sei'
    ),
    collapsed_prices as (
        select price_date, max(price) as price from prices group by price_date
    ),
    msg_atts_base AS (
        SELECT
            block_id,
            block_timestamp,
            tx_succeeded,
            tx_id,
            msg_group,
            msg_index,
            msg_type,
            inserted_timestamp,
            attribute_key,
            attribute_value,
            case 
                when attribute_key = 'contract_address' then attribute_value
                when msg_type = 'transfer' then 'sei_token_transfer'
                when msg_type = 'ibc_transfer' then 'sei_ibc_transfer'
                when msg_type = 'message' AND attribute_key = 'action' AND attribute_value = '/seiprotocol.seichain.oracle.MsgAggregateExchangeRateVote'
                    then 'sei_oracle_votes'
                when msg_type = 'instantiate' AND attribute_key = 'contract_address' then 'sei_create_contract'
                when msg_type = 'message' AND attribute_key = 'action' AND attribute_value = '/cosmos.gov.v1beta1.MsgVote' then 'sei_governance_votes'
                when msg_type = 'message' and attribute_key = 'module' and attribute_key = 'staking' then 'sei_staking'
                when msg_type = 'withdraw_rewards' then 'sei_staking_withdraw'
                when msg_type = 'coin_spent' then 'sei_staking_coin_spent'
                when msg_type in ('delegate','redelegate','unbond','create_validator') then 'sei_staking'
                else null
            end as contract_address
        FROM
            sei_flipside.core.fact_msg_attributes
        WHERE
            tx_succeeded
            {% if is_incremental() %}
            AND inserted_timestamp >= (
                SELECT
                    MAX(inserted_timestamp)
                FROM
                    {{ this }}
            )
            {% endif %}
    ),
    transaction_contract_data as (
        SELECT
            tx_id as tx_hash
            , min_by(block_timestamp, msg_index) as block_timestamp
            , min_by(tx_succeeded, msg_index) as tx_succeeded
            , min_by(contract_address, msg_index) as contract_address
            , min_by(msg_type, msg_index) as msg_type
            , min_by(attribute_value, msg_index) as attribute_value
            , min_by(inserted_timestamp, msg_index) as inserted_timestamp
        FROM
            msg_atts_base
        GROUP BY tx_id
    )
    SELECT 
        t1.tx_hash
        , t1.block_timestamp
        , date_trunc('day', t1.block_timestamp) as raw_date
        , t1.tx_succeeded
        , t1.contract_address
        , t3.name
        , t3.app
        , t3.friendly_name
        , t3.sub_category
        , t3.category
        , t1.msg_type
        , t1.attribute_value
        , t2.tx_from as signer
        , (split(t2.fee, 'usei')[0] / pow(10, 6)) as tx_fee
        , (split(t2.fee, 'usei')[0] / pow(10, 6)) * t4.price as gas_usd
        , t1.inserted_timestamp
        , null as user_type
        , null as address_life_span
        , null as cur_total_txns
        , null as cur_distinct_to_address_count
        , null as probability
        , null as engagement_type
        , null as balance_usd
        , null as native_token_balance
        , null as stablecoin_balance
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
        ON date_trunc('day', t1.block_timestamp) = t4.price_date
    WHERE
        t1.block_timestamp < date(sysdate())
        {% if is_incremental() %}
        AND t2.inserted_timestamp >= (
            SELECT
                max(inserted_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
