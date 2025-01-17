{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

SELECT symbol, contract_address, num_decimals, coingecko_id, initial_supply FROM 
    (
        VALUES
            ('BUIDL', '0x2893ef551b6dd69f661ac00f11d93e5dc5dc0e99', 6, 'blackrock-usd-institutional-digital-liquidity-fund', 0),
            ('OUSG', '0xba11c5effa33c4d6f8f593cfa394241cfe925811', 18, 'ousg', 0),
            ('FOBXX', '0x408a634b8a8f0de729b48574a3a7ec3fe820b00a', 18, 'franklin-onchain-u-s-government-money-fund', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
