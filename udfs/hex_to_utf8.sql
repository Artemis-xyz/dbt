CREATE OR REPLACE FUNCTION PC_DBT_DB.PROD.HEX_TO_UTF8("hex_str" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'hex_to_utf8'
AS '
def hex_to_utf8(hex_str) -> str:
    if hex_str.startswith("0x"):
        hex_str = hex_str[2:]
    try:
        bytes_obj = bytes.fromhex(hex_str)
        return bytes_obj.decode("utf-8")
    except ValueError:
        return None
';