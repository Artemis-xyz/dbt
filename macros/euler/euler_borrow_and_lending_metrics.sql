{% macro euler_borrow_and_lending_metrics(chain) %}
    with
    vaults as (
        select
            block_timestamp::date as date
            , vault_address
            , asset_address
            , accumulated_fees as fee_amount_cumulative_raw
            , price
            , decimals
            , symbol
            , case 
                when accumulated_fees = 0 then 0 
                else accumulated_fees::float - lag(accumulated_fees::float) ignore nulls over (partition by vault_address order by block_timestamp) 
            end as fee_amount_raw
            , cash as available_amount_cumulative_raw
            , case 
                when cash = 0 then 0 
                else cash::float - lag(cash::float) ignore nulls over (partition by vault_address order by block_timestamp) 
            end as available_amount_raw
            , interest_accumulator as interest_amount_cumulative_raw
            , case 
                when interest_accumulator = 0 then 0 
                else interest_accumulator::float - lag(interest_accumulator::float) ignore nulls over (partition by vault_address order by block_timestamp) 
            end as interest_amount_raw
            , total_borrows as borrow_amount_cumulative_raw
            , case 
                when total_borrows = 0 then 0 
                else total_borrows::float - lag(total_borrows::float) ignore nulls over (partition by vault_address order by block_timestamp) 
            end as borrow_amount_raw
            , total_shares as lp_amount_cumulative_raw
            , case 
                when total_shares = 0 then 0 
                else total_shares::float - lag(total_shares::float) ignore nulls over (partition by vault_address order by block_timestamp) 
            end as lp_amount_raw
        from {{ ref("fact_euler_" ~ chain ~ "_event_VaultStatus") }}
    )
    , date_spine as (
        select
            date
        from {{ ref("dim_date_spine") }}
        where date < to_date(sysdate()) and date >= (select min(date) from vaults)
    )
    , unique_vaults as (
        select distinct
            vault_address
            , asset_address
            , decimals
            , symbol
        from vaults
    )
    , dates_and_vaults as (
        select 
            d.date
            , v.vault_address
            , v.asset_address
            , v.decimals
            , v.symbol
        from date_spine d
        cross join unique_vaults v
    )
    , vault_data as (
        select
            date
            , vault_address
            , asset_address
            , price
            , decimals
            , symbol
            , sum(coalesce(fee_amount_raw, 0) / power(10, decimals)) as fee_amount_native
            , sum(sum(coalesce(fee_amount_raw, 0) / power(10, decimals))) over (partition by vault_address order by date) as fee_amount_cumulative_native
            , sum(coalesce(available_amount_raw, 0) / power(10, decimals)) as available_amount_native
            , sum(sum(coalesce(available_amount_raw, 0) / power(10, decimals))) over (partition by vault_address order by date) as available_amount_cumulative_native
            , sum(coalesce(available_amount_raw, 0)/ power(10, decimals) + coalesce(borrow_amount_raw, 0)/ power(10, decimals)) as supplied_amount_native
            , sum(sum(coalesce(available_amount_raw, 0) / power(10, decimals) + coalesce(borrow_amount_raw, 0)/ power(10, decimals))) over (partition by vault_address order by date) as supplied_amount_cumulative_native
            , sum(coalesce(interest_amount_raw / power(10, 27), 0)) as interest_amount_native
            , sum(sum(coalesce(interest_amount_raw / power(10, 27), 0))) over (partition by vault_address order by date) as interest_amount_cumulative_native
            , sum(coalesce(borrow_amount_raw, 0) / power(10, decimals)) as borrow_amount_native
            , sum(sum(coalesce(borrow_amount_raw, 0) / power(10, decimals))) over (partition by vault_address order by date) as borrow_amount_cumulative_native
            , sum(coalesce(lp_amount_raw, 0) / power(10, decimals)) as lp_amount_native
            , sum(sum(coalesce(lp_amount_raw, 0) / power(10, decimals))) over (partition by vault_address order by date) as lp_amount_cumulative_native

            , price * sum(coalesce(fee_amount_raw, 0) / power(10, decimals)) as fee_amount
            , price * sum(sum(coalesce(fee_amount_raw, 0) / power(10, decimals))) over (partition by vault_address order by date) as fee_amount_cumulative
            , price * sum(coalesce(available_amount_raw, 0) / power(10, decimals)) as available_amount
            , price * sum(sum(coalesce(available_amount_raw, 0) / power(10, decimals))) over (partition by vault_address order by date) as available_amount_cumulative
            , price * sum(coalesce(available_amount_raw, 0)/ power(10, decimals) + coalesce(borrow_amount_raw, 0)/ power(10, decimals)) as supplied_amount
            , price * sum(sum(coalesce(available_amount_raw, 0) / power(10, decimals) + coalesce(borrow_amount_raw, 0)/ power(10, decimals))) over (partition by vault_address order by date) as supplied_amount_cumulative
            , price * sum(coalesce(interest_amount_raw / power(10, 27), 0)) as interest_amount
            , price * sum(sum(coalesce(interest_amount_raw / power(10, 27), 0))) over (partition by vault_address order by date) as interest_amount_cumulative
            , price * sum(coalesce(borrow_amount_raw, 0) / power(10, decimals)) as borrow_amount
            , price * sum(sum(coalesce(borrow_amount_raw, 0) / power(10, decimals))) over (partition by vault_address order by date) as borrow_amount_cumulative
            , price * sum(coalesce(lp_amount_raw, 0) / power(10, decimals)) as lp_amount
            , price * sum(sum(coalesce(lp_amount_raw, 0) / power(10, decimals))) over (partition by vault_address order by date) as lp_amount_cumulative
        from vaults
        group by date, vault_address, asset_address, price, decimals, symbol
    )
     , filled_vault_data as (
        select
            d.date
            , coalesce(v.vault_address, d.vault_address) as vault_address
            , coalesce(v.asset_address, d.asset_address) as asset_address
            , coalesce(v.price, last_value(v.price ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            )) as price
            , coalesce(v.decimals, d.decimals) as decimals
            , coalesce(v.symbol, d.symbol) as symbol
            -- Daily metrics (not filled)
            , v.fee_amount_native
            , v.available_amount_native
            , v.supplied_amount_native
            , v.interest_amount_native
            , v.borrow_amount_native
            , v.lp_amount_native
            , v.fee_amount
            , v.available_amount
            , v.supplied_amount
            , v.interest_amount
            , v.borrow_amount
            , v.lp_amount
            -- Cumulative metrics (forward filled)
            , last_value(v.fee_amount_cumulative_native ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            ) as fee_amount_cumulative_native
            , last_value(v.available_amount_cumulative_native ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            ) as available_amount_cumulative_native
            , last_value(v.supplied_amount_cumulative_native ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            ) as supplied_amount_cumulative_native
            , last_value(v.interest_amount_cumulative_native ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            ) as interest_amount_cumulative_native
            , last_value(v.borrow_amount_cumulative_native ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            ) as borrow_amount_cumulative_native
            , last_value(v.lp_amount_cumulative_native ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            ) as lp_amount_cumulative_native
            , last_value(v.fee_amount_cumulative ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            ) as fee_amount_cumulative
            , last_value(v.available_amount_cumulative ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            ) as available_amount_cumulative
            , last_value(v.supplied_amount_cumulative ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            ) as supplied_amount_cumulative
            , last_value(v.interest_amount_cumulative ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            ) as interest_amount_cumulative
            , last_value(v.borrow_amount_cumulative ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            ) as borrow_amount_cumulative
            , last_value(v.lp_amount_cumulative ignore nulls) over (
                partition by d.vault_address 
                order by d.date
                rows between unbounded preceding and current row
            ) as lp_amount_cumulative
        from dates_and_vaults d
        left join vault_data v 
            on d.date = v.date 
            and d.vault_address = v.vault_address
            and d.asset_address = v.asset_address
    )
    select
        date
        , '{{chain}}' as chain
        , vault_address
        , asset_address
        , price
        , decimals
        , symbol
        , fee_amount_native
        , available_amount_native
        , supplied_amount_native
        , interest_amount_native
        , borrow_amount_native
        , lp_amount_native
        , fee_amount
        , available_amount
        , supplied_amount
        , interest_amount
        , borrow_amount
        , lp_amount
        , fee_amount_cumulative
        , available_amount_cumulative
        , supplied_amount_cumulative
        , interest_amount_cumulative
        , borrow_amount_cumulative
        , lp_amount_cumulative
    from filled_vault_data

{% endmacro %}