{{config(
    materialized = 'table'
)}}

WITH dates AS (
    SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, DATE('2017-09-16')) AS date
    FROM TABLE(generator(rowcount => 3000))
    WHERE DATEADD(day, seq4(), '2017-09-16') <= CURRENT_DATE()
),
wallets AS (
    SELECT LOWER(column1) AS user_address
    FROM VALUES 
        ('0x98C63b7B319dFBDF3d811530F2ab9DfE4983Af9D'),
        ('0x75398564ce69B7498dA10a11ab06Fd8fF549001c'),
        ('0x5560d001f977df5e49ead7ab0bdd437c4ee3a99e'),
        ('0xbe6977e08d4479c0a6777539ae0e8fa27be4e9d6'),
        ('0xdAD22a85ef8310eF582b70e4051E543F3153e11F'),
        ('0xe0362f7445e3203a496f6f8b3d51cbb413b69be2'),
        ('0x5A8e77bC30948cc9A51aE4E042d96e145648BB4C'),
        ('0xe0b66bFc7344a80152BfeC954942E2926A6FcA80'),
        ('0xa42D0A18B834F52e41bEDdEaA2940165db3DA9a3'),
        ('0x276F695b3B2C7f24E7CF5b9d24e416a7f357aDb7'),
        ('0x5Eab1966D5F61E52C22D0279F06f175e36A7181E'),
        ('0x959815462EeC5fFf387A2e8a6871d94323D371de'),
        ('0xb9b012cad0A7C1b10CbE33a1B3F623b06fAD1c7C'),
        ('0xfB682b0dE4e0093835EA21cfABb5449cA9ac9e5e'),
        ('0x3264225f2Fd3bb8D5DC50587EA7506aA8638B966'),
        ('0x8d34d66bDb2d1d6ACd788A2d73d68e62282332e7'),
        ('0x4a87ecE3eFffCb012fbE491AA028032e07B6F6cF'),
        ('0x57Ec4745258e5A4C73d1A82636dc0FE291e3eE9F'),
        ('0x37398A324d35c942574650B9eD2987BC640BAD76'),
        ('0xD321948212663366503E8dCCDE39cc8e71C267c0'),
        ('0x55b0ba1994d68C2AB0C01C3332eC9473de296137'),
        ('0xD48133C96C5FE8d41D0cbD598F65bf4548941e27'),
        ('0x9c17f630DBde24eECe8fd248fAA2E51f690FF79B'),
        ('0x35a5dc3FD1210Fe7173aDD3C01144Cf1693B5E45'),
        ('0x0DFfD343C2D3460a7EAD2797a687304Beb394ce0'),
        ('0x76287e0F7b107d1C9f8f01D5aFac314Ea8461a04'),
        ('0x9BBb46637A1Df7CADec2AFcA19C2920CdDCc8Db8'),
        ('0x7594Eb0ca0a7f313bEFD59AfE9e95c2201a443e4'),
        ('0x8652Fb672253607c0061677bDCaFb77a324DE081'),
        ('0x157235A3cc6011d9C26A010875c2550246aAbcCA'),
        ('0xa71bbBd288a4e288CfDC08bb2E70DCd74Da4486D'),
        ('0xEc640A90e9A30072158115B7C0253f2689ee6547'),
        ('0x2a6AB3B0C96377bd20AE47E50ae426A8546A4Ae9')
),
calendar AS (
    SELECT d.date, w.user_address
    FROM dates d
    CROSS JOIN wallets w
),
latest_balances AS (
    SELECT
        user_address,
        block_timestamp::date AS date,
        balance,
        ROW_NUMBER() OVER (PARTITION BY user_address, block_timestamp::date ORDER BY block_timestamp DESC) AS rn
    FROM {{ source("ETHEREUM_FLIPSIDE", "fact_token_balances" ) }} 
    WHERE lower(contract_address) = lower('0x514910771AF9Ca656af840dff83E8264EcF986CA')
      AND lower(user_address) IN (SELECT user_address FROM wallets)
),
filtered_balances AS (
    SELECT date, user_address, balance
    FROM latest_balances
    WHERE rn = 1
),
combined AS (
    SELECT 
        c.date,
        c.user_address,
        f.balance
    FROM calendar c
    LEFT JOIN filtered_balances f
        ON c.user_address = f.user_address AND c.date = f.date
),
filled_balances AS (
    SELECT 
        date,
        user_address,
        LAST_VALUE(balance IGNORE NULLS) OVER (
            PARTITION BY user_address ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS carried_balance
    FROM combined
),
supply_view AS (
    SELECT 
        date,
        1000000000.0 as max_supply_to_date,
        0.0 as uncreated_tokens,
        1000000000.0 as total_supply,
        0.0 as cumulative_burned_link,
        COALESCE(SUM(carried_balance) / 1e18, 0) as foundation_wallet_balance,
        1000000000.0 - COALESCE(SUM(carried_balance) / 1e18, 0) as issued_supply,
        1000000000.0 - COALESCE(SUM(carried_balance) / 1e18, 0) AS floating_supply
    FROM filled_balances
    GROUP BY date
),
coingecko_data AS (
    SELECT 
        date,
        SHIFTED_TOKEN_CIRCULATING_SUPPLY AS coingecko_circulating_supply
    FROM {{ ref("fact_coingecko_token_date_adjusted_gold") }} 
    WHERE coingecko_id = 'chainlink'
)
SELECT 
    s.date,
    s.max_supply_to_date,
    s.uncreated_tokens,
    s.total_supply,
    s.cumulative_burned_link,
    s.foundation_wallet_balance,
    s.issued_supply,
    s.floating_supply,
    c.coingecko_circulating_supply
FROM supply_view s
LEFT JOIN coingecko_data c
    ON s.date = c.date
ORDER BY s.date