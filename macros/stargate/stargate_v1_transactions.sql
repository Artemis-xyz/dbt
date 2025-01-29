{% macro rolling_active_addresses(chain) %}

-- TODO add input token column
with
    pools as (
        select LOWER(address) AS address
        from (
            values
            {% if chain == 'arbitrum' %}
            ('0x915A55e36A01285A14f05dE6e81ED9cE89772f8e'),
            ('0x892785f33CdeE22A30AEF750F285E18c18040c3e'),
            ('0xB6CfcF89a7B22988bfC96632aC2A9D6daB60d641'),
            ('0xaa4BF442F024820B2C28Cd0FD72b82c63e66F56C'),
            ('0xF39B7Be294cB36dE8c510e267B82bb588705d977'),
            ('0x600E576F9d853c95d58029093A16EE49646F3ca5')
            
            {% elif chain == 'avalanche' %}
            ('0x1205f31718499dBf1fCa446663B532Ef87481fe1'),
            ('0x29e38769f23701A2e4A8Ef0492e19dA4604Be62c'),
            ('0x1c272232Df0bb6225dA87f4dEcD9d37c32f63Eea'),
            ('0x8736f92646B2542B3e5F3c63590cA7Fe313e283B'),
            ('0xEAe5c2F6B25933deB62f754f239111413A0A25ef')

            {% elif chain == 'base' %}
            ('0x28fc411f9e1c480AD312b3d9C60c22b965015c6B'),
            ('0x4c80E24119CFB836cdF0a6b53dc23F04F7e652CA')

            {% elif chain == 'bsc' %}
            ('0x9aA83081AA06AF7208Dcc7A4cB72C94d057D2cda'),
            ('0x98a5737749490856b401DB5Dc27F522fC314A4e1'),
            ('0x4e145a589e4c03cBe3d28520e4BF3089834289Df'),
            ('0x7BfD7f2498C4796f10b6C611D9db393D3052510C'),
            ('0x68C6c27fB0e02285829e69240BE16f32C5f8bEFe')

            {% elif chain == 'ethereum' %}
            ('0x101816545F6bd2b1076434B54383a1E633390A2E'),
            ('0xdf0770dF86a8034b3EFEf0A1Bb3c889B8332FF56'),
            ('0x38EA452219524Bb87e18dE1C24D3bB59510BD783'),
            ('0x692953e758c3669290cb1677180c64183cEe374e'),
            ('0x0Faf1d2d3CED330824de3B8200fc8dc6E397850d'),
            ('0xfA0F307783AC21C39E939ACFF795e27b650F6e68'),
            ('0x590d4f8A68583639f215f675F3a259Ed84790580'),
            ('0xE8F55368C82D38bbbbDb5533e7F56AfC2E978CC2'),
            ('0x9cef9a0b1bE0D289ac9f4a98ff317c33EAA84eb8'),
            ('0xd8772edBF88bBa2667ed011542343b0eDDaCDa47'),
            ('0x430Ebff5E3E80A6C58E7e6ADA1d90F5c28AA116d'),
            ('0xa572d137666dcbadfa47c3fc41f15e90134c618c')

            {% elif chain == 'optimism' %}
            ('0xd22363e3762cA7339569F3d33EADe20127D5F98C'),
            ('0xDecC0c09c3B5f6e92EF4184125D5648a66E35298'),
            ('0x165137624F1f692e69659f944BF69DE02874ee27'),
            ('0x368605D9C6243A80903b9e326f1Cddde088B8924'),
            ('0x2F8bC9081c7FCFeC25b9f41a50d97EaA592058ae'),
            ('0x3533F5e279bDBf550272a199a223dA798D9eff78'),
            ('0x5421FA1A48f9FF81e4580557E86C7C0D24C1803')

            {% else %}
            (NULL) -- Fallback

            {% endif %}
        ) AS addresses(address)
    ),

    event_signatures as (
        select *
        from (
            values
            ('Mint(address,uint256,uint256,uint256)'),
            ('Burn(address,uint256,uint256)'),
            ('Swap(uint16,uint256,address,uint256,uint256,uint256,uint256,uint256)'),
            ('SwapRemote(address,uint256,uint256,uint256)')
        ) AS signatures(string)
    ),

    event_names as (
        select LEFT(string, CHARINDEX('(', string) - 1) AS name
        from event_signatures
    )

select
    contract_address,
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    decoded_log
from '{{ chain }}'_flipside.core.fact_decoded_event_logs
where 1=1
    and contract_address in (select address from pools)
    and event_name in (select name from event_names)
{% endmacro %}