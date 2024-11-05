CREATE OR REPLACE FUNCTION PC_DBT_DB.PROD.HEX_STRING_TO_EVM_ADDRESS("HEX_STRING" VARCHAR(16777216))
RETURNS VARCHAR(42)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'HEX_STRING_TO_EVM_ADDRESS'
AS '
def HEX_STRING_TO_EVM_ADDRESS(hex_string):
    """
    Converts a hex string to a blockchain address by prepending ''0x''.
    
    Args:
        hex_string (str): The input hex string to convert
        
    Returns:
        str: The blockchain address with ''0x'' prefix
        
    Example:
        Input: "1234abcd"
        Output: "0x1234abcd"
    """
    # Handle null input
    if hex_string is None:
        return None
        
    # Remove any existing ''0x'' prefix to avoid duplication
    hex_string = hex_string.lower().replace(''0x'', '''')
    
    # Return the formatted address
    return f"0x{hex_string}"    
';
