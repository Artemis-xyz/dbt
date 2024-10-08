{{
    config(
        materialized='incremental',
        snowflake_warehouse='jito'
    )
}}


SELECT 
    *
FROM {{ source('SOLANA_FLIPSIDE', 'fact_transfers') }} t
WHERE tx_to IN ('96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5' -- all the tip payment accounts 
                ,'HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe'
                ,'Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY'
                ,'ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49'
                ,'DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh'
                ,'ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt'
                ,'DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL'
                ,'3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT')
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(day)) from {{ this }})
{% endif %}
