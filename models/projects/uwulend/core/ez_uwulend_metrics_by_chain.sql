{{
    config(
        materialized="table",
        snowflake_warehouse="UWULEND",
        database="uwulend",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    uwulend_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_uwu_lend_ethereum_borrows_deposits_gold"),
                ],
            )
        }}
    )
select
    uwulend_by_chain.date
    , 'uwulend' as artemis_id
    , uwulend_by_chain.chain

    --Usage Data
    , uwulend_by_chain.daily_borrows_usd as lending_loans
    , uwulend_by_chain.daily_supply_usd as lending_deposits

from uwulend_by_chain
where uwulend_by_chain.date < to_date(sysdate())