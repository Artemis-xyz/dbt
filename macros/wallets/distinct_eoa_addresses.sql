{% macro distinct_eoa_addresses(chain) %}
    {% if chain == "tron" %}
        select distinct trx_from_address as address, 'eoa' as address_type
        from sonarx_tron.tron_share.transactions
        where trx_from_address is not null 
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