{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="fact_optimism_private_sale_emissions",
    )
}}

SELECT * FROM (VALUES
    (
        '2024-03-07',
        'Private Token Sale',
        19.5 * 1e6,
        'https://gov.optimism.io/t/token-sale-march-2024/7760/1'
    ),
    (
        '2023-09-20',
        'Private Token Sale',
        116 * 1e6,
        'https://gov.optimism.io/t/token-sale-september-2023/6846'
    )
) AS t (date, event_type, amount, source)