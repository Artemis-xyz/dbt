CREATE OR REPLACE FUNCTION HEX_TO_BASE58("HEX_STRING" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'hex_to_base58'
AS '
def hex_to_base58(hex_string):
    alphabet = ''123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz''

    num = 0

    if(hex_string != ''''):
        num = int(hex_string, 16)

    if num == 0:
        return ''1''

    base58 = ''''
    while num > 0:
        num, rem = divmod(num, 58)
        base58 = alphabet[rem] + base58

    return base58
';

CREATE OR REPLACE FUNCTION BASE58_TO_DECIMAL("ENCODED_STR" VARCHAR(16777216))
RETURNS NUMBER(38,0)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'base58_to_decimal'
AS '
def base58_to_decimal(encoded_str):

    def big_endian_to_little_endian(hex_num):
        big = bytearray.fromhex(hex_num)
        big.reverse()
        little = ''''.join(f"{n:02X}" for n in big)
        return little

    base58_alphabet = ''123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz''

    # Decode base58 string to integer
    num = 0
    for char in encoded_str:
        num *= 58
        if char not in base58_alphabet:
            raise ValueError("Invalid character in base58 string")
        num += base58_alphabet.index(char)

    # Convert integer to hex
    hex_str = hex(num)[2:]  # [2:] to strip the ''0x'' prefix

    # Ensure the hex string has even length
    if len(hex_str) % 2:
        hex_str = ''0'' + hex_str

    big_endian = hex_str[16:22]

    little_endian = big_endian_to_little_endian(big_endian)

    num = int(little_endian, 16)

    return num
';

CREATE OR REPLACE FUNCTION BASE58_TO_HEX("ENCODED_STR" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'base58_to_hex'
AS '
def base58_to_hex(encoded_str):

    base58_alphabet = ''123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz''
    hex_str = ""
    # Decode base58 string to integer
    num = 0
    if(encoded_str is not None):
        for char in encoded_str:
                num *= 58
                if char not in base58_alphabet:
                    raise ValueError("Invalid character in base58 string")
                num += base58_alphabet.index(char)

    # Convert integer to hex
    hex_str = hex(num)[2:]  # [2:] to strip the ''0x'' prefix

    # Ensure the hex string has even length
    if len(hex_str) % 2:
        hex_str = ''0'' + hex_str

    return hex_str
';

CREATE OR REPLACE FUNCTION PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL("BIG_ENDIAN" VARCHAR(16777216))
RETURNS NUMBER(38,0)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'big_endian_hex_to_decimal'
AS '
def big_endian_hex_to_decimal(big_endian):

    def big_endian_to_little_endian(hex_num):
        big = bytearray.fromhex(hex_num)
        big.reverse()
        little = ''''.join(f"{n:02X}" for n in big)
        return little


    little_endian = big_endian_to_little_endian(big_endian)

    num = int(little_endian, 16)

    return num
';