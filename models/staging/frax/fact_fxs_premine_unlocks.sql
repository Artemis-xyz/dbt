{{ config(materialized="table") }}

SELECT
  DATE_TRUNC('DAY', block_timestamp) as date,
  sum(amount) as premine_unlocks
FROM
  {{ source("ETHEREUM_FLIPSIDE_CORE", "ez_token_transfers") }}
WHERE
  contract_address = lower('0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0')
  AND from_address in (
    lower('0x8D4392F55bC76A046E443eb3bab99887F4366BB0'),
    lower('0xa95f86fE0409030136D6b82491822B3D70F890b3'),
    lower('0x874a873e4891fB760EdFDae0D26cA2c00922C404'),
    lower('0x11cC283d06FA762061df2B0D2f0787651ceef659')
  )
GROUP BY
  1
ORDER BY
  1 DESC