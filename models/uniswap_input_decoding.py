import sys

import snowflake.snowpark.functions as F
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
    elif name.startswith('fixed['):
        return 'fixed128x128' + name[5:]
    elif name == 'fixed':
        return 'fixed128x128'
    elif name.startswith('ufixed['):
        return 'ufixed128x128' + name[6:]
    elif name == 'ufixed':
        return 'ufixed128x128'
    else:
        return name

def parseType(type):

    if is_array(type):
        return "array type"
    else:
        raw_type = None
        if type == 'address':
            raw_type = 'uint160'
        elif type == 'bool':
            raw_type = 'uint8'
        elif type == 'string':
            raw_type = 'bytes'

        ret = {
            'rawType': raw_type,
            'name': type,
            'memoryUsage': 32,
            'size': 0
        }

        if (type.startswith('bytes') and type != 'bytes') or type.startswith('uint') or type.startswith('int'):
            ret['size'] = parse_type_n(type)
        if type.startswith('bytes') and type != 'bytes' and (ret['size'] < 1 or ret['size'] > 32):
            return "invalid type"
        if (type.startswith('uint') or type.startswith('int')) and (ret['size'] % 8 or ret['size'] < 8 or ret['size'] > 256):
            return "invalid type"
        return ret

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

def decodeSingle(parsed_type, data, offset):
    # base case, if type is bytes, uint, or int
    # if type(parsed_type) == "string":
    #     parsed_type = parseType(parsed_type)

    name = parsed_type["name"]
    if name.startswith('bytes'):
        return data.slice(offset, offset + parsed_type["size"])
    elif name.startswith('uint'):
        bytes_to_read = 32
        characters_to_read = 32 * 2
        # uint_stripped = data[offset:offset + characters_to_read].lstrip("0")
        # decoded_num = get_big_num(uint_stripped, 16)
        # return decoded_num, characters_to_read
        try:
            uint_stripped = data[offset:offset + characters_to_read].lstrip("0")
            decoded_num = int(uint_stripped, 16)
            return decoded_num, characters_to_read
        except:
            return "unable to decode uint", characters_to_read
    elif name == "address":
        bytes_to_read = 32
        characters_to_read = 32 * 2
        decoded_num = "0x" + data[offset + 24:offset + characters_to_read].lstrip("0")
        return decoded_num, characters_to_read
    elif name == "bool":
        bytes_to_read = 32
        characters_to_read = 32 * 2
        decoded_bool = data[offset:offset + characters_to_read][-1]
        if decoded_bool == "0":
            return "False", characters_to_read
        else:
            return "True", characters_to_read
    # elif name.startswith('int'):
    #     num = BN(data.slice(offset, offset + 32), 16, 'be').fromTwos(256)
    #     return num
    # elif name.startswith('bytes'):
    #     return data.slice(offset, offset + parsed_type["size"])

def decode_input(input, hashed_signature):
    open_paranthesis = hashed_signature.find("(")
    closed_paranthesis = hashed_signature.find(")")
    types = hashed_signature[int(open_paranthesis + 1):int(closed_paranthesis)].split(",")
    start_read_index = 0
    modified_types = []
    without_method_id_data = input[10:]  # strip method ID
    for type in types:
        elementary_type = elementary_name(type)
        parsed_type = parseType(elementary_type)
        decoded_input, characters_read = decodeSingle(parsed_type, without_method_id_data, start_read_index)
        modified_types.append(decoded_input)
        start_read_index += characters_read
    return modified_types

def model(dbt, session):
    dbt.config(materialized="table", packages = ["pandas", "regex"])
    transactions_with_method_abis_df = dbt.ref("raw_transactions_with_method_fragment")

    decoded_df = transactions_with_method_abis_df.to_pandas()
    decoded_df = decoded_df.loc[decoded_df['HASHABLE_SIGNATURE'] == "setApprovalForAll(address,bool)"]
    decoded_df = decoded_df.head(100)
    elementary_names = []
    for index, row in decoded_df.iterrows():
        result = decode_input(row['INPUT'], row['HASHABLE_SIGNATURE'])
        elementary_names.append(result)
    decoded_df["decoded_input"] = elementary_names
    return decoded_df
