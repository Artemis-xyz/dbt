{% macro address_balances(chain) %}
    with
        credit_debit as (
            select address, contract_address, block_timestamp, credit as flow
            from pc_dbt_db.prod.fact_{{ chain }}_address_credit_by_token
            {% if is_incremental() %}
                where block_timestamp > (select max(block_timestamp) from {{ this }})
            {% endif %}
            union all
            select address, contract_address, block_timestamp, debit as flow
            from pc_dbt_db.prod.fact_{{ chain }}_address_debit_by_token
            {% if is_incremental() %}
                where block_timestamp > (select max(block_timestamp) from {{ this }})
            {% endif %}
            {% if chain in ('optimism') %}
            -- Some chains had a re-genesis event, if this is the case we need to include the genesis balances

                union all
                select address, contract_address, block_timestamp, balance as flow
                from {{ref("fact_"~chain~"_genesis_stablecoin_balances")}}
                {% if is_incremental() %}
                    where block_timestamp > (select max(block_timestamp) from {{ this }})
                {% endif %}
            {% endif %}
            
        ),
        credit_debits_and_latest_balances as (
            select address, contract_address, block_timestamp, flow
            from credit_debit
            {% if is_incremental() %}
                union all
                select address, contract_address, block_timestamp, token_balance as flow
                from prod.dim_{{ chain }}_current_balances
            {% endif %}
        )
    select
        address,
        contract_address,
        block_timestamp,
        sum(flow) over (
            partition by contract_address, address order by block_timestamp
        ) as balance_token
    from credit_debits_and_latest_balances

{% endmacro %}


{% macro address_balances_with_flipside_ez(chain) %}
    select
        user_address as address,
        case
            when contract_address is null then 'native_token' else contract_address
        end as contract_address,
        block_timestamp,
        case
            when contract_address is null then current_bal else current_bal_unadj
        end as balance_token
    from {{ chain }}_flipside.core.ez_balance_deltas
    where
        to_date(block_timestamp) < to_date(sysdate())
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
{% endmacro %}
