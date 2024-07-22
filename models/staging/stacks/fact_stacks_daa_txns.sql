with
    max_extraction as (
        select max(extraction_date) as max_date
        from landing_database.prod_landing.raw_stacks_daa_txns
    ),
    data as (
        select parse_json(source_json) data
        from landing_database.prod_landing.raw_stacks_daa_txns
        where extraction_date = (select max_date from max_extraction)
    ),
    stack_date as (
        select index as id, value::date as date
        from data, lateral flatten(input => data:day)
    ),
    stack_daa as (
        select index as id, value::double as daa
        from data, lateral flatten(input => data:daa)
    ),
    stack_txns as (
        select index as id, value::double as txns
        from data, lateral flatten(input => data:txns)
    ),
    stack_date_txns as (
        select date, txns from stack_date left join stack_txns using (id)
    ),
    stack_date_daa as (select date, daa from stack_date left join stack_daa using (id)),
    stacks_before_12_27_data as (
        select stack_date_daa.date, 'stacks' as chain, daa, txns
        from stack_date_daa
        join stack_date_txns using (date)
        where daa is not null and txns is not null
    ),
    stacks_after_12_27 as (
        select
            date_trunc('day', block_timestamp)::date as date,
            'stacks' as chain,
            count(distinct sender_address) as daa,
            count(*) as txns
        from {{ ref("fact_stacks_transactions") }}
        group by date
    ),
    combined_stacks as (
        select *
        from stacks_before_12_27_data
        union all
        select *
        from stacks_after_12_27
    )
select date, max(chain) as chain, max(daa) as daa, max(txns) as txns
from combined_stacks
where date < date_trunc('DAY', sysdate())
group by date
