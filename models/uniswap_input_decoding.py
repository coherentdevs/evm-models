import sys

import snowflake.snowpark.functions as F
import gmpy2
import regex
import pandas as pd


def elementary_name(name):
    if name.startswith('int['):
        return 'int256' + name[3:]
    elif name == 'int':
        return 'int256'
    elif name.startswith('uint['):
        return 'uint256' + name[4:]
    elif name == 'uint':
        return 'uint256'
    else:
        return name

def parseType(type):
    if is_array(type):
        type_remove_array = type[:len(type)-2]
        signatureNoParentheses = type_remove_array.replace('(', '').replace(')', '')
        ret = {
            'isArray': True,
            'name': "array",
            "arrayType": signatureNoParentheses.split(","),
        }
        return ret
    else:
        return {'isArray': False, 'name': type}

def parse_type_n(type):
    match = regex.match(r'^\D+(\d+)$', type)
    if match:
        return int(match.group(1))
    else:
        return None

def is_array(type):
    if type[-1] == "]":
        return True
    else:
        return False
def decode_uint(data, offset):
    bytes_to_read = 32
    characters_to_read = bytes_to_read * 2
    uint_stripped = data[offset:offset + characters_to_read].lstrip("0")
    if len(uint_stripped) == 0:
        return "0", characters_to_read
    add_0x = "0x" + uint_stripped
    large_int = gmpy2.mpz(add_0x)
    return gmpy2.digits(large_int), characters_to_read

def decodeSingle(parsed_type, data, offset, dynamic_bytes_data_start = 0):
    # base case, if type is bytes, uint, or int
    # if type(parsed_type) == "string":
    #     parsed_type = parseType(parsed_type)

    name = parsed_type["name"]
    if name == "array": # currently works on arrays that dont have nested tuples, for arrays with dynamic types, remove array length and offset
        decoded_array = []
        length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2 # finds where in data length of array is stored
        length = int(gmpy2.mpz("0x" + data[length_offset: length_offset + 64], 16).digits(10)) # length of array
        dynamic_bytes_data_start = length_offset + 64 # in case we are reading bytes, data array begins here
        array_offset = length_offset + 64

        for i in range(0, length):
            for type in parsed_type['arrayType']:
                new_input = {'isArray': False, 'name': type}
                if type == "bytes" or type == "string":
                    result, characters_read = decodeSingle(new_input, data, array_offset, dynamic_bytes_data_start = dynamic_bytes_data_start)
                else:
                    result, characters_read = decodeSingle(new_input, data, array_offset)
                decoded_array.append(result)
                array_offset += characters_read
        return str(decoded_array), 64
    # if name == "tuple":
    #     decoded_tuple = ()
    #     for value in parsed_type:
    #         decoded_tuple = (decodeSingle(parseType["tupleType"], data, tuple_read))
    #     return decoded_tuple


    elif name.startswith('bytes') and name != "bytes": # bytes32
        bytes_to_read = 32
        characters_to_read = bytes_to_read * 2
        return "0x" + data[offset:offset + characters_to_read], characters_to_read
    elif name.startswith('uint'):
        decoded_uint, characters_read = decode_uint(data, offset)
        return decoded_uint, characters_read
    elif name.startswith('int'):
        bytes_to_read = 32
        characters_to_read = bytes_to_read * 2
        int_block = data[offset:offset + characters_to_read]
        first_hex = int_block[0]
        if int(gmpy2.mpz("0x" + first_hex, 16).digits(10)) >= 8: # negative
            # twos complement
            # 1. convert input to binary
            # 2. invert all bits
            # 3. add one
            # 4. convert back to base ten with negative sign
            base_2 = gmpy2.mpz("0x" + int_block, 16).digits(2)

            flipped_binary_str = ""
            for bit in base_2:
                flipped_bit = '0' if bit == '1' else '1'
                flipped_binary_str += flipped_bit
            incremented_binary_result = gmpy2.mpz(flipped_binary_str, 2) + 1

            return "-" + gmpy2.digits(incremented_binary_result, 10), characters_to_read
        else: # treat as unsigned int
            decoded_uint, characters_read = decode_uint(data, offset)
            return decoded_uint, characters_read
    elif name == "address":
        bytes_to_read = 32
        characters_to_read = bytes_to_read * 2
        decoded_num = "0x" + data[offset + 24:offset + characters_to_read] # address starts at the 12th byte
        return decoded_num, characters_to_read
    elif name == "bool":
        bytes_to_read = 32
        characters_to_read = bytes_to_read * 2
        decoded_bool = data[offset:offset + characters_to_read][-1] # get last bit
        if decoded_bool == "0":
            return "False", characters_to_read
        else:
            return "True", characters_to_read
    elif name == "string":
        length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
        # if string is part of an array, bytes index begins after array signatures
        length_start = dynamic_bytes_data_start + length_offset
        length_end = length_start + 64
        length = int(gmpy2.mpz("0x" + data[length_start: length_end], 16).digits(10)) * 2
        if length == 0: return "", 64
        string_start = length_end # string always begins right at length end
        return bytes.fromhex(data[string_start: string_start + length]).decode('utf-8'), 64
    elif name == "bytes":
        length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
        # if bytes is part of an array, bytes index begins after array signatures
        length_start = dynamic_bytes_data_start + length_offset
        length_end = length_start + 64
        length = int(gmpy2.mpz("0x" + data[length_start: length_end], 16).digits(10)) * 2
        if length == 0: return "0x", 64
        string_start = length_end # string always begins right at length end
        return "0x" + data[string_start: string_start + length], 64
    else:
        return "unknown type detected, unable to decode", 64

def decode_input(input, hashed_signature):
    open_parenthesis = hashed_signature.find("(")
    closed_parenthesis = hashed_signature.rfind(")")
    types = hashed_signature[int(open_parenthesis + 1):int(closed_parenthesis)].split(",")
    offset = 0
    decoded_inputs = []
    stripped_method_id_data = input[10:]  # strip method ID
    for type in types:
        elementary_type = elementary_name(type)
        parsed_type = parseType(elementary_type)
        try:
            decoded_input, characters_read = decodeSingle(parsed_type, stripped_method_id_data, offset)
        except:
            decoded_input = "unable to decode " + parsed_type["name"]
            characters_read = 64

        decoded_inputs.append(decoded_input)
        offset += characters_read
    return decoded_inputs

def model(dbt, session):
    dbt.config(materialized="table", packages = ["pandas", "regex", "gmpy2"])
    transactions_with_method_abis_df = dbt.ref("raw_transactions_with_method_fragment")

    decoded_df = transactions_with_method_abis_df.to_pandas()
    decoded_df = decoded_df.loc[decoded_df['HASHABLE_SIGNATURE'] == "propose(address[],uint256[],bytes[],string)"]
    decoded_df = decoded_df.head(100)
    elementary_names = []
    for index, row in decoded_df.iterrows():
        result = decode_input(row['INPUT'], row['HASHABLE_SIGNATURE'])
        elementary_names.append(result)
    decoded_df["decoded_input"] = elementary_names
    return decoded_df
