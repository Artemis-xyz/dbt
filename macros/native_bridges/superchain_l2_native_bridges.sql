{% macro get_superchain_l2_native_bridges_flipside(chain) %}
    select
        block_timestamp
        , tx_hash
        , event_index
        , coalesce(decoded_log:"from"::string, decoded_log:"_from"::string) as depositor
        , coalesce(decoded_log:"to"::string, decoded_log:"_to"::string) as recipient
        , TO_NUMERIC(coalesce(decoded_log:"amount", decoded_log:"_amount")) as amount_native
        , null as fee
        , case 
            when event_name = 'WithdrawalInitiated' and  coalesce(decoded_log:"_l2Token", decoded_log:"l2Token") = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' then '0x4200000000000000000000000000000000000006'
            when event_name = 'WithdrawalInitiated' and  coalesce(decoded_log:"_l2Token", decoded_log:"l2Token") <> '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' then coalesce(decoded_log:"_l2Token", decoded_log:"l2Token")::string
            when event_name = 'DepositFinalized' and coalesce(decoded_log:"_l1Token", decoded_log:"l1Token") = '0x0000000000000000000000000000000000000000'
            then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            when event_name = 'DepositFinalized' and coalesce(decoded_log:"_l1Token", decoded_log:"l1Token") <> '0x0000000000000000000000000000000000000000' then coalesce(decoded_log:"_l1Token", decoded_log:"l1Token")::string
        end as src_token_address
        , case 
            when event_name = 'DepositFinalized' and  coalesce(decoded_log:"_l2Token", decoded_log:"l2Token") = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' then '0x4200000000000000000000000000000000000006'
            when event_name = 'DepositFinalized' and  coalesce(decoded_log:"_l2Token", decoded_log:"l2Token") <> '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' then coalesce(decoded_log:"_l2Token", decoded_log:"l2Token")::string
            when event_name = 'WithdrawalInitiated' and coalesce(decoded_log:"_l1Token", decoded_log:"l1Token") = '0x0000000000000000000000000000000000000000'
            then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            when event_name = 'WithdrawalInitiated' and coalesce(decoded_log:"_l1Token", decoded_log:"l1Token") <> '0x0000000000000000000000000000000000000000' then coalesce(decoded_log:"_l1Token", decoded_log:"l1Token")::string
        end as dst_token_address
        , case 
            when event_name = 'WithdrawalInitiated' then '{{chain}}' 
            when event_name = 'DepositFinalized' then 'ethereum' 
        end as source_chain
        , case 
            when event_name = 'WithdrawalInitiated' then 'ethereum'
            when event_name = 'DepositFinalized' then '{{chain}}' 
        end as destination_chain
        , decoded_log
        , event_name
        , contract_address
        , case 
            when event_name = 'WithdrawalInitiated' then 'withdrawal'
            when event_name = 'DepositFinalized' then 'deposit' 
        end as action
    from {{chain}}_flipside.core.ez_decoded_event_logs
    where
        contract_address = lower('0x4200000000000000000000000000000000000010')
        and event_name in ('WithdrawalInitiated', 'DepositFinalized')
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
{% endmacro %}
