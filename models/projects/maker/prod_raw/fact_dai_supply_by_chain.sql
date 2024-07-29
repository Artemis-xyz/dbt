{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_dai_supply_by_chain"
    )
}}


{{
dbt_utils.union_relations(
    relations=[
        ref("fact_dai_eth_supply"),
        ref("fact_dai_dsr_supply"),
    ]
)
}}