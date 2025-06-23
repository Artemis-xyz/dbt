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
            {% if chain in ('tron') %}
            -- TRON USDT supply for the address below is negative to begin with, this means its first transfer is out 
            -- not in, the data at the beginning of tron is pretty iffy and the block explorer seems to fail the closer you
            -- get to the genesis block. it is only max negative by $10 over its history so I am giving it an inital supply of 10000000/1e6 USDT
            -- THPvaUhoh2Qn2y9THCZML3H815hhFhn5YC is the contract creator
            -- https://tronscan.org/#/tools/advanced-filter?type=transfer&secondType=20&times=1530417600000%2C1556769599999&fromAddress=THPvaUhoh2Qn2y9THCZML3H815hhFhn5YC&toAddress=THPvaUhoh2Qn2y9THCZML3H815hhFhn5YC&token=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t&imgUrl=https%3A%2F%2Fstatic.tronscan.org%2Fproduction%2Flogo%2Fusdtlogo.png&tokenName=Tether%20USD&tokenAbbr=USDT&relation=or
                union all
                select  address, contract_address, block_timestamp, flow
                from (
                    values
                        (
                            'THPvaUhoh2Qn2y9THCZML3H815hhFhn5YC'
                            , 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t'
                            , '2019-04-16 07:00:00.000'::timestamp
                            , 10000000
                        )
                    ) as t(address, contract_address, block_timestamp, flow)
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
-- Maker does not use ERC20 DAI tokens in the protocol, instead the amount associated with an addresses is stored in the VAT contract
-- This amount can be transfered to the POT contract(0x197e90f9fad81970ba7976f33cbd77088e5d7cf7) to earn yield on your DAI. 
    {% if chain in ('ethereum') %}
        with 
            ethereum_maker_vat_dai_debit as (
                select
                    lower('0x197e90f9fad81970ba7976f33cbd77088e5d7cf7') AS address
                    , lower('0x6B175474E89094C44Da98b954EedeAC495271d0F') AS contract_address
                    , block_timestamp
                    , rad::double * 1e18 AS delta
                from ethereum_flipside.maker.fact_VAT_move
                where dst_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
                {% if is_incremental() %}
                    and block_timestamp > (select max(block_timestamp) from {{ this }})
                {% endif %}
                union all
                select
                    lower('0x197e90f9fad81970ba7976f33cbd77088e5d7cf7') AS address
                    , lower('0x6B175474E89094C44Da98b954EedeAC495271d0F') AS contract_address
                    , block_timestamp
                    , - rad::double * 1e18 AS delta
                from ethereum_flipside.maker.fact_VAT_move
                where lower(src_address) = lower('0x197e90f9fad81970ba7976f33cbd77088e5d7cf7')
                {% if is_incremental() %}
                    and block_timestamp > (select max(block_timestamp) from {{ this }})
                {% endif %}
                union all
                select
                    lower('0x197e90f9fad81970ba7976f33cbd77088e5d7cf7') as address
                    , lower('0x6B175474E89094C44Da98b954EedeAC495271d0F') AS contract_address
                    , block_timestamp
                    , rad::double * 1e18 AS delta
                from ethereum_flipside.maker.fact_VAT_suck
                where lower(v_address) = lower('0x197e90f9fad81970ba7976f33cbd77088e5d7cf7')
                {% if is_incremental() %}
                    and block_timestamp > (select max(block_timestamp) from {{ this }})
                {% endif %}
            )
            , ethereum_maker_vat_dai_balances as (
                select address, contract_address, block_timestamp, delta
                from ethereum_maker_vat_dai_debit
                {% if is_incremental() %}
                    union all
                    select
                        address
                        , contract_address
                        , max(block_timestamp) as block_timestamp
                        , max_by(balance_token, block_timestamp) as delta
                    from {{this}}
                    where lower(address) = lower('0x197e90f9fad81970ba7976f33cbd77088e5d7cf7')
                        and lower(contract_address) = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')
                    group by address, contract_address
                {% endif %}
            )
    {% endif %} 
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
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    {% if chain in ('ethereum') %}
    union all
        select
            address
            , contract_address
            , block_timestamp
            , sum(delta) over (partition by contract_address, address order by block_timestamp) as balance_token
        from ethereum_maker_vat_dai_balances
    {% endif %}
    -- Add genesis ETH balances
    {% if chain in ('ethereum') and not is_incremental() %}
    union all
    select
        to_address as address
        , 'native_token' as contract_address
        , block_timestamp
        , value_raw / 1e18 as balance_token
    FROM {{ref('fact_ethereum_genesis_transactions')}}
    {% endif %}
{% endmacro %}
