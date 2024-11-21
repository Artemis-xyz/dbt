{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

SELECT symbol, issuer, type FROM 
    (
        VALUES
            ('BUIDL', 'BlackRock', 'Treasury'),
            ('TBILL', 'OpenEden', 'Treasury'),
            ('USDY', 'Ondo', 'Treasury'),
            ('OUSG', 'Ondo', 'Treasury'),
            ('USYC', 'Ondo', 'Treasury'),
            ('FOBXX', 'Franklin Templeton', 'Treasury'),
            ('PAXG', 'Paxos', 'Gold')
    ) as results(symbol, issuer, type)