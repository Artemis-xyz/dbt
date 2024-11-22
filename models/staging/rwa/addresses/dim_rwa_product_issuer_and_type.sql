{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

SELECT symbol, issuer, product_type FROM 
    (
        VALUES
            ('BUIDL', 'BlackRock', 'Treasury'),
            ('TBILL', 'OpenEden', 'Treasury'),
            ('USDY', 'Ondo', 'Treasury'),
            ('OUSG', 'Ondo', 'Treasury'),
            ('USYC', 'Hashnote', 'Treasury'),
            ('FOBXX', 'Franklin Templeton', 'Treasury'),
            ('PAXG', 'Paxos', 'Gold'),
            ('XAUT', 'Tether', 'Gold')
    ) as results(symbol, issuer, product_type)
