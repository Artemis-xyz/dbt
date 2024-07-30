{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="dim_maker_contracts"
    )
}}

SELECT * FROM (VALUES
        ( 'FlapFlop', '0x4d95a049d5b0b7d32058cd3f2163015747522e99' )
        , ( 'FlapFlop', '0xa4f79bc4a5612bdda35904fdf55fc4cb53d1bff6' )
        , ( 'FlapFlop', '0x0c10ae443ccb4604435ba63da80ccc63311615bc' )
        , ( 'FlapFlop', '0xa41b6ef151e06da0e34b009b86e828308986736d' )
        , ( 'FlapFlop', '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' )
        , ( 'PSM', '0x961ae24a1ceba861d1fdf723794f6024dc5485cf' )
        , ( 'PSM', '0x204659b2fd2ad5723975c362ce2230fba11d3900' )
        , ( 'PSM', '0x89b78cfa322f6c5de0abceecab66aee45393cc5a' )
    ) AS t(contract_type, contract_address)
