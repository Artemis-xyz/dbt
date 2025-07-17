{% macro distinct_eoa_addresses(chain) %}
    {% if chain == "tron" %}
        select COALESCE(trx_from_address, trx_owner_address) as address, 'eoa' as address_type, max(datetime) as last_updated_at
        from sonarx_tron.tron_share.transactions
        where trx_from_address is not null 
        {% if is_incremental() %}
            and datetime > (select max(last_updated_at) from {{this}})
        {% endif %}
        group by 1
    {% elif chain == "hyperevm" %}
        select
            parquet_raw:from_address::string as address,
            max(parquet_raw:block_timestamp::timestamp_ntz) as last_updated_at
        from {{ source("PROD_LANDING", "raw_hyperevm_transactions_parquet") }}
        where parquet_raw:from_address::string is not null
        {% if is_incremental() %}
            and datetime > (select max(last_updated_at) from {{this}})
        {% endif %}
        group by 1
    {% elif chain == "ripple" %}
        select address, 'eoa' as address_type, max(last_updated_at) as last_updated_at
        from (
            select account as address, max(datetime) as last_updated_at
            from sonarx_xrp.xrp_share.transactions
            where account is not null 
            {% if is_incremental() %}
                and datetime > (select max(last_updated_at) from {{ this }})
            {% endif %}
            group by 1
            union all
            select from_address as address, max(block_timestamp) as last_updated_at
            from {{ ref('fact_ripple_token_transfers') }}
            where from_address is not null
            {% if is_incremental() %}
                and block_timestamp > (select max(last_updated_at) from {{ this }})
            {% endif %}
            group by 1
            union all
            select to_address as address, max(block_timestamp) as last_updated_at
            from {{ ref('fact_ripple_token_transfers') }}
            where to_address is not null
            {% if is_incremental() %}
                and block_timestamp > (select max(last_updated_at) from {{ this }})
            {% endif %}
            group by 1
        ) all_eoa_address
        group by 1
    {% elif chain == "sui" %}
        select distinct from_address as address, 'eoa' as address_type
        from {{ref('fact_sui_token_transfers')}}
        where from_address is not null and not (from_address like '%POOL%')
    {% elif chain == "ton" %}
        select distinct transaction_account as address, 'wallet' as address_type
        from {{ref("fact_ton_transactions")}}
        where transaction_account_workchain <> -1 and transaction_account_interface ilike '%wallet%'
    {% elif chain == "solana" %}
        select distinct signer as address,  'signer' as address_type
        from solana_flipside.core.ez_signers 
    {% elif chain == "near" %}
        select distinct tx_signer as address, 'signer' as address_type
        from near_flipside.core.fact_transactions
        --tx_singer is a contract: https://flipsidecrypto.github.io/near-models/#!/model/model.near.core__fact_actions_events_function_call
        -- receipt_receiver_id is a contract: https://flipsidecrypto.github.io/near-models/#!/model/model.near_models.core__ez_actions
        where tx_signer not in (select receipt_receiver_id from near_flipside.core.ez_actions where action_data:method_name is not null)
    {% endif %}
{% endmacro %}