{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

SELECT symbol, issuer_id, issuer_friendly_name, product_type FROM 
    (
        VALUES
            ('BUIDL', 'blackrock', 'BlackRock', 'Treasury'),
            ('TBILL', 'openeden', 'OpenEden', 'Treasury'),
            ('USDY', 'ondo', 'Ondo', 'Treasury'),
            ('OUSG', 'ondo', 'Ondo', 'Treasury'),
            ('USYC', 'hashnote', 'Hashnote', 'Treasury'),
            ('FOBXX', 'franklin_templeton', 'Franklin Templeton', 'Treasury'),
            ('PAXG', 'paxos', 'Paxos', 'Gold'),
            ('XAUT', 'tether', 'Tether', 'Gold')
    ) as results(symbol, issuer_id, issuer_friendly_name, product_type)
