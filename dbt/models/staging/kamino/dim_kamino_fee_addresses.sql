SELECT
    fee_vault as vault_name,
    token_account_address
FROM (
    VALUES 
        ('SOL', '3JNof8s453bwG5UqiXBLJc77NRQXezYYEBbk3fqnoKph'),
        ('JitoSOL', 'C2PyjpFRtbQjFjHNB3HDcoQoLP7VJ9NQn6NFJZMueWfB'),
        -- ... (rest of the values)
        ('GRASS', 'EVTRSdajwqWfCRGam9MaK268UT9bVKdrrhDKBJh2pKDT')
) as fee_addresses(fee_vault, token_account_address)
