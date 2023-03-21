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
        type_remove_array = type[:-2]
        signatureNoParanthesis = type_remove_array.replace('(', '').replace(')', '')
        ret = {
            'isArray': True,
            'name': "array",
            "arrayType": signatureNoParanthesis.split(","),
        }
        return ret
    else:
        # raw_type = None
        # if type == 'address':
        #     raw_type = 'uint160'
        # elif type == 'bool':
        #     raw_type = 'uint8'
        # elif type == 'string':
        #     raw_type = 'bytes'
        #
        # ret = {
        #     'rawType': raw_type,
        #     'name': type,
        #     'memoryUsage': 32,
        #     'size': 0
        # }
        #
        # if (type.startswith('bytes') and type != 'bytes') or type.startswith('uint') or type.startswith('int'):
        #     ret['size'] = parse_type_n(type)
        # if type.startswith('bytes') and type != 'bytes' and (ret['size'] < 1 or ret['size'] > 32):
        #     return "invalid type"
        # if (type.startswith('uint') or type.startswith('int')) and (ret['size'] % 8 or ret['size'] < 8 or ret['size'] > 256):
        #     return "invalid type"
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

def decodeSingle(parsed_type, data, offset):
    # base case, if type is bytes, uint, or int
    # if type(parsed_type) == "string":
    #     parsed_type = parseType(parsed_type)

    name = parsed_type["name"]
    # 0x7fd30df0
    # 0000000000000000000000000000000000000000000000000000000000000040
    # 0000000000000000000000000000000000000000000000000000000000000080
    # 0000000000000000000000000000000000000000000000000000000000000001
    # 000000000000000000000000954c2641c354bf8d8170592b9ad7016238162c1d
    # 0000000000000000000000000000000000000000000000000000000000000001
    # 000000000000000000000000000000000000000000000000000000398258e600

    # 0xf40ace34
    # 8b7bab60690779b1ddb369c11e6ae462aa62cf83c0eb1945dedf4562946f4102
    # 0000000000000000000000006fc13eace26590b80cccab1ba5d51890577d83b2
    # 00000000000000000000000065a8f07bd9a8598e1b5b6c0a88f4779dbc077675
    # 0000000000000000000000000000000000000000000012b9b46cec39b79a6ca1
    # 00000000000000000000000000000000000000000000000000000000000000e0
    # 0000000000000000000000000000000000000000000000000000000000000140
    # 00000000000000000000000000000000000000000000000000000000000001a0
    # 0000000000000000000000000000000000000000000000000000000000000002
    # 000000000000000000000000000000000000000000000000000000000000001b
    # 000000000000000000000000000000000000000000000000000000000000001c
    # 0000000000000000000000000000000000000000000000000000000000000002
    # 3689ac10804ab2f255540615f730f7d5c7fe74a655b1e021049577cbc5627bef
    # cf259650f8a5e3f4c7e4b55f5468d055788ef27cfb324f6e3fd43ee7ba24b854
    # 0000000000000000000000000000000000000000000000000000000000000002
    # 16e601f23c4e1bcc0e1805ef9025f89a558d0ec124cebe679163d1659cfaa44c
    # 2cda19f433891c69afb75a1f4107b724bb67e54a8de7a80884e4b52281cb5744
    if name == "array":
        decoded_array = []
        length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
        length = int(gmpy2.mpz("0x" + data[length_offset: length_offset + 64], 16).digits(10))
        array_read = length_offset + 64
        for i in range(0, length):
            for type in parsed_type['arrayType']:
                new_input = {'isArray': False, 'name': type}
                result, characters_read = decodeSingle(new_input, data, array_read)
                decoded_array.append(result)
                array_read += characters_read

        return decoded_array, 64
    elif name.startswith('bytes') and name != "bytes": # bytes32
        try:
            bytes_to_read = 32
            characters_to_read = bytes_to_read * 2
            return "0x" + data[offset:offset + characters_to_read], characters_to_read
        except:
            return "unable to decode bytes32", characters_to_read
    elif name.startswith('uint'):
        bytes_to_read = 32
        characters_to_read = bytes_to_read * 2
        try:
            uint_stripped = data[offset:offset + characters_to_read].lstrip("0")
            if len(uint_stripped) == 0:
                return "0", characters_to_read
            add_0x = "0x" + uint_stripped
            large_int = gmpy2.mpz(add_0x)
            return gmpy2.digits(large_int), characters_to_read
        except:
            return "unable to decode uint", characters_to_read
    elif name.startswith('int'):
        bytes_to_read = 32
        characters_to_read = bytes_to_read * 2
        input_block = data[offset:offset + characters_to_read]
        first_hex = input_block[0]
        if int(gmpy2.mpz("0x" + first_hex, 16).digits(10)) >= 8: # negative
            try:
                base_2 = gmpy2.mpz("0x" + input_block, 16).digits(2)
                flipped_binary_str = ""

                for bit in base_2:
                    flipped_bit = '0' if bit == '1' else '1'
                    flipped_binary_str += flipped_bit
                incremented_binary_result = gmpy2.mpz(flipped_binary_str, 2) + 1

                return "-" + gmpy2.digits(incremented_binary_result, 10), characters_to_read
            except:
                return "unable to decode int", characters_to_read
        else:
            try:
                int_stripped = data[offset:offset + characters_to_read].lstrip("0")
                if len(int_stripped) == 0:
                    return "0", characters_to_read
                add_0x = "0x" + int_stripped
                large_int = gmpy2.mpz(add_0x)
                return gmpy2.digits(large_int), characters_to_read
            except:
                return "unable to decode int", characters_to_read
    elif name == "address":
        bytes_to_read = 32
        characters_to_read = bytes_to_read * 2
        decoded_num = "0x" + data[offset + 24:offset + characters_to_read]
        return decoded_num, characters_to_read
    elif name == "bool":
        bytes_to_read = 32
        characters_to_read = 32 * 2
        decoded_bool = data[offset:offset + characters_to_read][-1]
        if decoded_bool == "0":
            return "False", characters_to_read
        else:
            return "True", characters_to_read

    # 0x9120491c
    # 0000000000000000000000000000000000000000000000000000000000000020
    # 0000000000000000000000000000000000000000000000000000000000000002
    # 000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    # 0000000000000000000000000000000000000000000000000011c37937e08000
    # 0000000000000000000000003a5bd1e37b099ae3386d13947b6a90d97675e5e3
    # 000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    # 0000000000000000000000000000000000000000000000000000e35fa931a000
    # 000000000000000000000000de21f729137c5af1b01d73af1dc21effa2b8a0d6

    # 0x000055be
    # 0000000000000000000000000000000000000000000000000000000000000040
    # 0000000000000000000000000000000000000000000000000000000000000080
    # 0000000000000000000000000000000000000000000000000000000000000001
    # 0000000000000000000000002b31d64d4a76328db235dc3a83be951aa10d8fe7
    # 0000000000000000000000000000000000000000000000000000000000000001
    # 000000000000000000000000000000000000000000000000001550f7dca70000

    elif name == "string":
        try:
            length_start = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
            length = int(gmpy2.mpz("0x" + data[length_start: length_start + 64], 16).digits(10)) * 2
            if length == 0: return "", 64
            string_start = length_start + 64
            return bytes.fromhex(data[string_start: string_start + length]), 64
        except:
            return "unable to decode string", 64
    elif name == "bytes":
        try:
            length_start = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
            length = int(gmpy2.mpz("0x" + data[length_start: length_start + 64], 16).digits(10)) * 2
            if length == 0: return "0x", 64
            string_start = length_start + 64
            return "0x" + data[string_start: string_start + length], 64
        except:
            return "unable to decode bytes", 64
    else:
        return "unknown type detected, unable to decode", 64

        # return length_start, 64
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
    dbt.config(materialized="table", packages = ["pandas", "regex", "gmpy2"])
    transactions_with_method_abis_df = dbt.ref("raw_transactions_with_method_fragment")

    decoded_df = transactions_with_method_abis_df.to_pandas()
    decoded_df = decoded_df.loc[decoded_df['HASHABLE_SIGNATURE'] == "upgradeBoxes(uint256[],uint256[],uint256[])"]
    decoded_df = decoded_df.head(100)
    elementary_names = []
    for index, row in decoded_df.iterrows():
        result = decode_input(row['INPUT'], row['HASHABLE_SIGNATURE'])
        elementary_names.append(result)
    decoded_df["decoded_input"] = elementary_names
    return decoded_df
