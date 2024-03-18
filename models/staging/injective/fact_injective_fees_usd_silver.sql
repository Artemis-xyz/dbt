with
    prices as (
        select date, coingecko_id, shifted_token_price_usd as price
        from {{ source("PC_DBT_DB.PROD", "fact_coingecko_token_date_adjusted_gold") }}
        where
            coingecko_id in (
                select distinct (coingecko_id)
                from
                    {{
                        source(
                            "PC_DBT_DB.PROD", "fact_injective_fees_native_all_silver"
                        )
                    }}
            )
    )
select p.date as date, sum(p.price * f.fees_native_all) as fees, 'injective' as chain
from {{ source("PC_DBT_DB.PROD", "fact_injective_fees_native_all_silver") }} f
left join prices p on f.date = p.date and f.coingecko_id = p.coingecko_id
group by p.date
order by p.date desc
