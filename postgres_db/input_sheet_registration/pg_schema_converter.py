import re
from decimal import Decimal
from typing import Dict
from Input_sheet_converter import process_files
def parse_range_block(text):
    if ',' in text:
        # It's a fixed list like [3,4,5]
        return {
            'type': 'list',
            'values': text.split(',')
        }
    else:
        # It's a range like [10-400/1]
        try:
            start, end_step = text.split('-')
            end, step = end_step.split('/')
            return {
                'type': 'range',
                'start': float(start),
                'end': float(end),
                'step': float(step)
            }
        except Exception as e:
            raise ValueError(f"Invalid range block format: [{text}]") from e


def parse_PartNumber(PartNumber: str) -> Dict:
    """Parse PartNumber and return metadata"""
    regex = ''
    ranges = []
    static_parts = []
    pattern = re.compile(r'\[(.*?)\]')
    pos = 0

    for match in pattern.finditer(PartNumber):
        start, end = match.span()
        static_text = PartNumber[pos:start]
        static_parts.append(static_text)
        regex += re.escape(static_text)

        range_text = match.group(1)
        range_dict = parse_range_block(range_text)
        ranges.append(range_dict)
        regex += r'([\d.]+)'
        pos = end

    static_text = PartNumber[pos:]
    if static_text:
        static_parts.append(static_text)
        regex += re.escape(static_text)

    return {
        'PartNumber': PartNumber,
        'regex': f'^{regex}$',
        'ranges': ranges,
        'static_parts': static_parts,
    }


input_folder = r"C:\Users\user\Downloads\test_folder"

for item in process_files(input_folder):
    PartNumber_core_data = parse_PartNumber(item['PartNumber'])
    PartNumber = PartNumber_core_data.get('PartNumber')
    first_static_part_of_PartNumber = PartNumber_core_data.get('static_parts')[0]
    regex_of_PartNumber = PartNumber_core_data.get('regex')
    ranges_of_PartNumber = PartNumber_core_data.get('ranges')
    category = item.get('Category')
    brand = item.get('Brand')
    PartNumber_type_input = item.get('PartNumber_Type_input')
    env_value = item.get('Env_value')
    mapper = item.get('mapper')
    print(item)
