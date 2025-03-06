{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="fact_optimism_private_sale_emissions",
    )
}}

SELECT 
    -- dateadd('year', 2,
        '2024-03-07'
            -- ) 
            as date, 
    'Private Token Sale' as event_type, 
    19.5 * 1e6 as amount, 
    'https://gov.optimism.io/t/token-sale-march-2024/7760/1' as source
UNION ALL 
SELECT 
    -- dateadd('year', 2, 
        '2023-09-20'
            -- )
            as date, 
    'Private Token Sale' as event_type, 
    116 * 1e6 as amount, 
'https://gov.optimism.io/t/token-sale-september-2023/6846' as source