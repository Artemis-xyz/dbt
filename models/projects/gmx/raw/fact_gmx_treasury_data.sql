{{
    config(
        materialized="table",
        snowflake_warehouse="GMX",
        database="gmx",
        schema="raw",
        alias="fact_gmx_treasury_data",
    )
}}

with treasury_arbitrum as (
    {{ get_treasury_balance(
            chain = 'arbitrum',
            addresses = [
                '0x68863dDE14303BcED249cA8ec6AF85d4694dea6A'
            ],
            earliest_date = '2021-06-01'
        )
    }}
), treasury_avalanche as (
    {{ get_treasury_balance(
            chain = 'avalanche',
            addresses = [
                '0x0339740d92fb8BAf73bAB0E9eb9494bc0Df1CaFD'
            ],
            earliest_date = '2021-06-01'
        )
    }}
), treasury_ethereum as (
    {{ get_treasury_balance(
            chain = 'ethereum',
            addresses = [
                '0x2706AA4532721e6bCe2eA21c3Bb5bbb2146d1Ef1',
                '0x6D42dAf7C26Aa3780178b80FAa893b9B6d4cCb85'
            ],
            earliest_date = '2021-06-01'
        )
    }}
)
select * from treasury_arbitrum
union all
select * from treasury_avalanche
union all
select * from treasury_ethereum