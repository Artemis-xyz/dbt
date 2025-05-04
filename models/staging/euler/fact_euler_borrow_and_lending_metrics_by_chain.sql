{{
    config(snowflake_warehouse="EULER", materialized="table")
}}

{{
    dbt_utils.union_relations(
        relations=[
            ref("fact_euler_avalanche_borrow_and_lending_metrics"),
            ref("fact_euler_base_borrow_and_lending_metrics"),
            ref("fact_euler_bsc_borrow_and_lending_metrics"),
            ref("fact_euler_ethereum_borrow_and_lending_metrics")
        ]
    )
}}