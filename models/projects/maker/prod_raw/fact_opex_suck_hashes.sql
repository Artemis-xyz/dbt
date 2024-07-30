{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_opex_suck_hashes"
    )
}}

SELECT suck.tx_hash
FROM ethereum_flipside.maker.fact_vat_suck suck
WHERE suck.u_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'
  AND suck.v_address IN (
    '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb',
    '0x2cc583c0aacdac9e23cb601fda8f1a0c56cdcb71',
    '0xa4c22f0e25c6630b2017979acf1f865e94695c4b'
  )
  AND suck.rad != 0
GROUP BY 1