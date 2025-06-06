import json
import re

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
                    f"When comparing this {key} with others, focus on identifying any overlapping "
                    f"or intersecting values within this {key} interval."
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
                        f"The acceptable values of {key} for the component can be one of {vals_str}. "
                        f"When comparing this {key} with others, focus on identifying any overlapping "
                        f"or intersecting values within this {key} interval."
                    )
                else:
                    output_json[key] = value
        else:
            output_json[key] = value

    json_str = json.dumps(output_json, indent=2)
    full_text = (
        "The following text is going to be converted to embeddings using openai text-embedding-3-large model. "
        "So embed the following text such that it can be compared as closely as possible:\n\n" + json_str
    )
    return full_text


# Example usage
input_json = {
    "type": "SFJ",
    "length": "[15-20/1]",
    "diameter": "[100-200/1]mm",
    "width": "[50-100/5] cm",
    "height": "[10,20,30]inches",
    "material": "steel"
}

prepared_text = specs_range_modifier_for_embedding(input_json)
print(prepared_text)
