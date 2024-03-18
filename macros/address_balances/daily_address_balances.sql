{% macro daily_address_balances(
    chain, chain_coingecko_id, wrapped_token_address, wrapped_token_decimals
) %}

    with
        token_balances as (
            select
                date_trunc('day', block_timestamp) as date,
                address,
                contract_address,
                {% if chain == "solana" %} avg(amount) as average_balance_token
                {% else %} avg(balance_token) as average_balance_token
                {% endif %}
            from prod.fact_{{ chain }}_address_balances_by_token
            where
                (
                    lower(contract_address)
                    in ('native_token', lower('{{wrapped_token_address}}'))
                    or lower(contract_address) in (
                        select lower(contract_address)
                        from prod.fact_{{ chain }}_stablecoin_contracts
                    )
                )
                {% if is_incremental() %}
                    and date >= coalesce(
                        (select dateadd('day', -3, max(date)) from {{ this }}),
                        '2023-01-01'
                    )
                {% endif %}
            group by date, address, contract_address
        ),

        native_token_price as (
            select
                date, 'native_token' as token_address, shifted_token_price_usd as price
            from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
            where coingecko_id = '{{chain_coingecko_id}}'
            union
            select
                dateadd('day', -1, date) as date,
                'native_token' as token_address,
                token_current_price as price
            from pc_dbt_db.prod.fact_coingecko_token_realtime_data
            where token_id = '{{chain_coingecko_id}}'
        ),

        token_prices as (
            select
                date,
                '{{wrapped_token_address}}' as token_address,
                max(price) as price,
                {{ wrapped_token_decimals }} as decimals
            from native_token_price
            group by date, native_token_price.token_address
            union
            select date, token_address, max(price) as price, 0 as decimals
            from native_token_price
            group by date, token_address
        ),

        address_token_balances as (
            select
                token_balances.date,
                token_balances.address,
                token_balances.contract_address,
                case
                    when
                        lower(token_balances.contract_address)
                        in ('native_token', lower('{{wrapped_token_address}}'))
                    then (average_balance_token / pow(10, decimals))
                    else (average_balance_token / pow(10, stable.num_decimals))
                end as average_balance_token,
                case
                    when
                        lower(token_balances.contract_address)
                        in ('native_token', lower('{{wrapped_token_address}}'))
                    then (average_balance_token / pow(10, decimals)) * price
                    else (average_balance_token / pow(10, stable.num_decimals)) * 1
                end as average_balance_usd,
                decimals,
                price
            from token_balances
            left join
                token_prices
                on token_balances.date = token_prices.date
                and lower(token_balances.contract_address)
                = lower(token_prices.token_address)
            left join
                prod.fact_{{ chain }}_stablecoin_contracts as stable
                on lower(token_balances.contract_address)
                = lower(stable.contract_address)
        )

    select
        date,
        address,
        case
            when sum(average_balance_usd) >= -.001 and sum(average_balance_usd) < 0
            then 0
            when sum(average_balance_usd) < -.001
            then null
            else sum(average_balance_usd)
        end as balance_usd,
        sum(
            case
                when
                    lower(contract_address) = 'native_token'
                    or lower(contract_address) = lower('{{wrapped_token_address}}')
                then average_balance_token
                else 0
            end
        ) as native_token_balance,
        sum(
            case
                when
                    lower(contract_address) in (
                        select lower(contract_address)
                        from prod.fact_{{ chain }}_stablecoin_contracts
                    )
                then average_balance_usd
                else 0
            end
        ) as stablecoin_balance
    from address_token_balances
    group by date, address

{% endmacro %}
