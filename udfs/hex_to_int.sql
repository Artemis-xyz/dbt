create or replace function hex_to_int(hex varchar)
returns varchar
language python runtime_version = '3.8'
handler = 'hex_to_int'
as
    $$
def hex_to_int(hex) -> str:
    """
    Converts hex (of any size) to int (as a string). Snowflake and java script can only handle up to 64-bit (38 digits of precision)
    hex_to_int('200000000000000000000000000000211');
    >> 680564733841876926926749214863536423441
    hex_to_int('0x200000000000000000000000000000211');
    >> 680564733841876926926749214863536423441
    hex_to_int(NULL);
    >> NULL
    """
    try:
        return str(int(hex, 16)) if hex and hex != "0x" else None
    except:
        return None
$$
;

create or replace function hex_to_int_with_encoding(encoding varchar, hex varchar)
returns varchar
language python runtime_version = '3.8'
handler = 'hex_to_int_with_encoding'
as
    $$
def hex_to_int_with_encoding(encoding, hex) -> str:
    """
    Converts hex (of any size) to int (as a string). Snowflake and java script can only handle up to 64-bit (38 digits of precision)
    hex_to_int('hex', '200000000000000000000000000000211');
    >> 680564733841876926926749214863536423441
    hex_to_int('hex', '0x200000000000000000000000000000211');
    >> 680564733841876926926749214863536423441
    hex_to_int('hex', NULL);
    >> NULL
    hex_to_int('s2c', 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffe5b83acf');
    >> -440911153
    """
    try:
        if not hex:
            return None
        if encoding.lower() == 's2c':
            if hex[0:2].lower() != '0x':
                hex = f'0x{hex}'

            bits = len(hex[2:]) * 4
            value = int(hex, 0)
            if value & (1 << (bits - 1)):
                value -= 1 << bits
            return str(value)
        else:
            return str(int(hex, 16))
    except:
        return None
$$
;
