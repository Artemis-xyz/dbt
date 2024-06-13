{{config(snowflake_warehouse="STACKS")}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from landing_database.prod_landing.raw_stacks_native_fees
    ),
    data as (
        select parse_json(source_json) data
        from landing_database.prod_landing.raw_stacks_native_fees
        where extraction_date = (select max_date from max_extraction)
    ),
    stack_date as (
        select index as id, value::date as date
        from data, lateral flatten(input => data:day)
    ),
    stack_fees as (
        select index as id, value::double as native_token_fees
        from data, lateral flatten(input => data:fees)
    ),
    stack_price as ({{ get_coingecko_price_with_latest("blockstack") }}),
    stack_date_fee_before_12_27 as (
        select date, native_token_fees, 'stacks' as chain
        from stack_date
        left join stack_fees using (id)
    ),
    stacks_date_fee_after_12_27 as (
        select
            date_trunc('day', block_timestamp)::date as date,
            sum(tx_fee) as native_token_fees,
            'stacks' as chain
        from {{ ref("fact_stacks_transactions") }}
        group by date
    ),
    combined_stacks as (
        select *
        from stack_date_fee_before_12_27
        union all
        select *
        from stacks_date_fee_after_12_27
    )
select
    combined_stacks.date,
    max(chain) as chain,
    max(native_token_fees) as native_token_fees,
    max(price) as price,
    max(native_token_fees * price) as fees
from combined_stacks
left join stack_price on combined_stacks.date = stack_price.date
where price is not null and combined_stacks.date < date_trunc('DAY', sysdate())
group by combined_stacks.date
