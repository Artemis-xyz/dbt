{% macro solana_stable_token_supply(
    stablecoin_contract_address, stablecoin_owner_address
) %}

    with
        stablecoin_mint as (
            select block_timestamp, mint_amount as amount, mint, decimal
            from solana_flipside.core.fact_token_mint_actions
            where mint = '{{ stablecoin_contract_address }}' and succeeded = 'TRUE'
            union all
            select block_timestamp, burn_amount * -1 as amount, mint, decimal
            from solana_flipside.core.fact_token_burn_actions
            where mint = '{{ stablecoin_contract_address }}' and succeeded = 'TRUE'
        ),
        stablecoin_mint_current as (
            select
                mint as contract_address,
                block_timestamp,
                sum(amount / pow(10, decimal)) over (
                    partition by mint order by block_timestamp
                ) as current_mint
            from stablecoin_mint
            order by block_timestamp desc
        ),
        daily_stablecoin_mint as (
            select
                contract_address,
                date_trunc('day', block_timestamp) as date,
                max_by(current_mint, block_timestamp) as token_minted
            from stablecoin_mint_current
            group by date, contract_address
        ),
        stablecoin_token_balance as (
            select
                address,
                date_trunc('day', block_timestamp) as date,
                contract_address,
                max_by(amount, block_timestamp) as token_burned
            from pc_dbt_db.prod.fact_solana_address_balances_by_token
            where
                address = '{{ stablecoin_owner_address }}'
                and contract_address = '{{ stablecoin_contract_address }}'
            group by address, date, contract_address
        ),
        combined_data as (
            select
                coalesce(burn.date, mint.date) as date,
                coalesce(
                    burn.contract_address, mint.contract_address
                ) as contract_address,
                token_burned,
                token_minted
            from stablecoin_token_balance as burn
            full join
                daily_stablecoin_mint as mint
                on burn.date = mint.date
                and burn.contract_address = mint.contract_address
        ),
        date_range as (
            select -1 + row_number() over (order by 0) i, start_date + i date
            from
                (
                    select min(date)::date as start_date, to_date(sysdate()) as end_date
                    from combined_data
                )
            join table(generator(rowcount => 10000)) x
            qualify i < 1 + end_date - start_date
        ),
        full_range_data as (
            select
                coalesce(date_range.date, combined_data.date) as date,
                '{{ stablecoin_contract_address }}' as contract_address,
                token_burned,
                token_minted
            from combined_data
            full join date_range on combined_data.date = date_range.date
        ),
        forward_fill_data as (
            select
                date,
                coalesce(
                    contract_address,
                    lag(contract_address) ignore nulls over (
                        partition by contract_address order by date
                    )
                ) as contract_address,
                coalesce(
                    token_burned,
                    lag(token_burned) ignore nulls over (
                        partition by contract_address order by date
                    )
                ) as token_burned,
                coalesce(
                    token_minted,
                    lag(token_minted) ignore nulls over (
                        partition by contract_address order by date
                    )
                ) as token_minted
            from full_range_data
        )
    select
        date,
        contract_address,
        token_burned,
        token_minted,
        coalesce(token_minted, 0) - iff(
            token_burned > 100 or token_burned is null,
            coalesce(token_burned, 0),
            lag(token_burned) ignore nulls over (
                partition by contract_address order by date
            )
        ) as total_supply
    from forward_fill_data
    where date < to_date(sysdate())

{% endmacro %}
