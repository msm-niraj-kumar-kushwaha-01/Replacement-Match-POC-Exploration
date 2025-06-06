import json
import re

def extract_spec_values(specs_json: dict, keys_string: str) -> dict:
    keys = keys_string.split(',')
    result = {}

    for key in keys:
        if key in specs_json and isinstance(specs_json[key], dict):
            result.update(specs_json[key])

    return result


def specs_range_modifier_for_embedding(input_json):
    output_json = {}
    for key, value in input_json.items():
        if isinstance(value, str):
            # Check for range pattern [start-end/step]unit
            range_match = re.match(r"\[(\d+)-(\d+)/(\d+)\](.*)", value)
            if range_match:
                start, end, step, unit = range_match.groups()
                unit = unit.strip()
                unit_text = f" {unit}" if unit else ""
                output_json[key] = (
                    f"The acceptable values of {key} range for the component is from "
                    f"{start}{unit_text} to {end}{unit_text}, increase by {step} mm increments. "
                )
            else:
                # Check for discrete list pattern [val1,val2,val3]unit
                list_match = re.match(r"\[([\d,]+)\](.*)", value)
                if list_match:
                    values, unit = list_match.groups()
                    unit = unit.strip()
                    unit_text = f" {unit}" if unit else ""
                    # Split the values by comma and add unit_text to each value
                    vals_with_unit = [v + unit_text for v in values.split(',')]
                    vals_str = ", ".join(vals_with_unit)
                    output_json[key] = (
                        f"The values of {key} can be one of {vals_str}. "
                    )
                else:
                    output_json[key] = value
        else:
            output_json[key] = value

    json_str = json.dumps(output_json, indent=2)
    
    return json_str

def query_maker_weaviate(specs_json: dict, keys_string: str):
    return specs_range_modifier_for_embedding(extract_spec_values(specs_json, keys_string))