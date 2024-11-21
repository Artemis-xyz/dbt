SELECT symbol, contract_address, num_decimals, coingecko_id, initial_supply FROM 
    (
        VALUES
            ('OUSG', 'i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc', 6, 'ousg', 0),
            ('USDY', 'A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6', 6, 'ondo-us-dollar-yield', 0),
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
