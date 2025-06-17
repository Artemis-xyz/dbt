{{ config(materialized="table") }}
-- Premint address can be an account owner or a token account
select contract_address, premint_address
from
    (
        values
            -- USDC
            (
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE'
            ),
            (
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                '3emsAVdmGKERbHjmGfQ6oZ1e35dkf5iYcS6U4CPKFVaa'
            ),
            (
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                'CcG5MeLP6b7maxMEcmucMkoeY9X37sJ5Wk7R97k9hJL9'
            ),
            (
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                '28VqfqsUUBx59i8ruG2TuC5RekW5ZY3tsK4bSV59sXjn'
            ),
            (
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                '27T5c11dNMXjcRuko9CeUy3Wq41nFdH3tz9Qt4REzZMM'
            ),
            (
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                'AoUnMozL1ZF4TYyVJkoxQWfjgKKtu8QUK9L4wFdEJick'
            ),
            (
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                'FSxJ85FXVsXSr51SeWf9ciJWTcRnqKFSmBgRDeL3KyWw'
            ),
            (
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                'DBD8hAwLDRQkTsu6EqviaYNGKPnsAMmQonxf7AH8ZcFY'
            ),
            -- USDT
            (
                'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
                '99pfYkuFEUPvUzVSGsMc2VN47FaFSTrKBMXGnxg13tDt'
            ),
            (
                'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
                'Q6XprfkF8RQQKoQVG33xT88H7wi8Uk1B1CC7YAs69Gi'
            ),
            (
                'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
                '8hnVkd24Gp7s1QYVPBQSayS4uLokKEj1Uq6NFjk6PibK'
            ),
            -- EURC
            (
                'HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr',
                '89LjSs8wP3EZNCKcs8aCxngQaEuyhUyGE4STkFwNgRT3'
            ),
            (
                'HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr',
                '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE'
            ),
            (
                'HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr',
                'HQHDva1bR4vqxvz6Y8yaLvqkD4AeWCDso8aJ9v5rfLHY'
            ),
            (
                'HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr',
                'CcG5MeLP6b7maxMEcmucMkoeY9X37sJ5Wk7R97k9hJL9'
            ),
            (
                'HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr',
                '6UpQvp85vJK3ZmR8VWnHJYTRaBz1maatVdec4sTJQVqP'
            ),
            -- AUSD
            (
                'AUSD1jCcCyPLybk1YnvPWsHQSrZ46dxwoMniN4N2UEB9',
                '5EWBgCRe581iGMdwEvvtzpUgVANewpz4JFmkLjpD3EKi'
            ),
            (
                'AUSD1jCcCyPLybk1YnvPWsHQSrZ46dxwoMniN4N2UEB9',
                'C44jAY8ufK2kdaFVdDuh6woQZzb6r9jZquHdFBQYUb95'
            )
    ) as results(contract_address, premint_address)
