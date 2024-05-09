{{
    config(
        materialized="table",
    )
}}

SELECT
    mint,
    count(*) as total_transfers
FROM solana_flipside.core.fact_transfers
where
    block_timestamp > date('2023-01-01')
group by 1
order by 2 desc
limit 500