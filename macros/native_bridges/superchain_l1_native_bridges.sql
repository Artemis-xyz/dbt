{% macro get_superchain_l1_native_bridges(contract_address, chain) %}

select
    block_timestamp
    , tx_hash
    , event_index
    , coalesce(decoded_log:"from", decoded_log:"_from") as depositor
    , coalesce(decoded_log:"to", decoded_log:"_to") as recipient
    , coalesce(decoded_log:"amount", decoded_log:"_amount") as amount
    , null as fee
    , case 
        when event_name = 'ERC20DepositInitiated' then coalesce(decoded_log:"l1Token", decoded_log:"localToken")  
        when event_name = 'ETHDepositInitiated' then  '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        when event_name = 'ERC20WithdrawalFinalized' then coalesce(decoded_log:"l2Token", decoded_log:"remoteToken")
        when event_name = 'ETHWithdrawalFinalized' then '0x4200000000000000000000000000000000000006'
    end as src_token_address
    , case 
        when event_name = 'ERC20DepositInitiated' then coalesce(decoded_log:"remoteToken", decoded_log:"l2Token")
        when event_name = 'ETHDepositInitiated' then '0x4200000000000000000000000000000000000006'
        when event_name = 'ERC20WithdrawalFinalized' then coalesce(decoded_log:"localToken", decoded_log:"l1Token")
        when event_name = 'ETHWithdrawalFinalized' then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    end as dst_token_address
    , case 
        when event_name = 'ERC20DepositInitiated' then '{{chain}}'
        when event_name = 'ETHDepositInitiated' then '{{chain}}'
        when event_name = 'ERC20WithdrawalFinalized' then 'ethereum' 
        when event_name = 'ETHWithdrawalFinalized' then 'ethereum'
    end as source_chain
    , case 
        when event_name = 'ERC20DepositInitiated' then 'ethereum'
        when event_name = 'ETHDepositInitiated' then 'ethereum'
        when event_name = 'ERC20WithdrawalFinalized' then '{{chain}}' 
        when event_name = 'ETHWithdrawalFinalized' then '{{chain}}'
    end as destination_chain
    , decoded_log
    , event_name
    , contract_address
from ethereum_flipside.core.fact_decoded_event_logs
where
    contract_address = lower('{{ contract_address }}')
    and event_name in ('ETHDepositInitiated', 'ETHWithdrawalFinalized', 'ERC20DepositInitiated', 'ERC20WithdrawalFinalized')
    {% if is_incremental() %}

        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

    {% endif %}
{% endmacro %}
