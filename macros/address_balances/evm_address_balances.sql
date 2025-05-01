{% macro evm_address_balances(chain) %}
    with
    credit_and_debit as (
        select
            block_timestamp
            , block_number
            , contract_address
            , address
            , credit_raw as flow_raw
            , credit_native as flow_native
        from {{ref("fact_"~chain~"_address_credits")}}
        {% if is_incremental() %}
            where block_timestamp > (select max(block_timestamp) from {{ this }})
        {% endif %}
        union all
        select
            block_timestamp
            , block_number
            , contract_address
            , address
            , debit_raw as flow_raw
            , debit_native as flow_native
        from {{ref("fact_"~chain~"_address_debits")}}
        {% if is_incremental() %}
            where block_timestamp > (select max(block_timestamp) from {{ this }})
        {% endif %}
    )
    , credit_and_debit_and_latest_balances as (
        select
            block_timestamp
            , block_number
            , contract_address
            , address
            , flow_raw
            , flow_native
        from credit_and_debit
        {% if is_incremental() %}
            union all
            select
                max(block_timestamp) as block_timestamp
                , max(block_number) as block_number
                , contract_address
                , address
                , max_by(balance_raw, block_timestamp) as flow_raw
                , max_by(balance_native, block_timestamp) as flow_native
            from {{ this }}
            group by address, contract_address
        {% endif %}
    )
    select
        block_timestamp
        , block_number
        , contract_address
        , address
        , sum(flow_raw) over (
            partition by contract_address, address order by block_number
        ) as balance_raw
        , sum(flow_native) over (
            partition by contract_address, address order by block_number
        ) as balance_native
    from credit_and_debit_and_latest_balances
{% endmacro %}