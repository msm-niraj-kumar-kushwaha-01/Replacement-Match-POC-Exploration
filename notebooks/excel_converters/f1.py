import pandas as pd
import re
from openpyxl import load_workbook

# Load workbook and skip rows above 7 (header=6 means row 7 is header, zero-indexed)
input_file = r"C:\Users\user\Downloads\20241022_GATESUNITTA_MISUMI_TimingBeltConveyor_EndlessType_01_Jyoti_Umar_complete.xlsx"  # Change to your filename
df = pd.read_excel(input_file, header=6, engine='openpyxl')

# Mapping of circled numbers to integers
symbol_map = {
    '①': '1', '②': '2', '③': '3', '④': '4', '⑤': '5',
    '⑥': '6', '⑦': '7', '⑧': '8', '⑨': '9', '⑩': '10'
}
symbol_pattern = re.compile('|'.join(re.escape(sym) for sym in symbol_map))

def safe_cast_value(val):
    """Casts to int if integer-like, float if decimal, else returns original string."""
    val_str = str(val).strip()
    if val_str.lower() in ['nan', '', 'none']:
        return ""
    try:
        if '.' in val_str:
            return float(val_str)
        return int(val_str)
    except:
        return val_str

def custom_mapper_expression(expr):
    expr = str(expr).strip()
    if not symbol_pattern.search(expr):
        return None  # No symbols, no mapping needed

    # Find first symbol
    match = symbol_pattern.search(expr)
    if not match:
        return None

    first_symbol = match.group(0)
    first_digit = symbol_map[first_symbol]

    # Remaining expression after removing first symbol
    remaining_expr = expr[:match.start()] + expr[match.end():]

    # Split remaining_expr into parts separated by circled symbols
    parts = symbol_pattern.split(remaining_expr)
    symbols = symbol_pattern.findall(remaining_expr)

    # Build quoted string with inserted quotes around digits
    # Start quote
    quoted_parts = ['"']
    for i, part in enumerate(parts):
        quoted_parts.append(part)
        if i < len(symbols):
            # close quote, add digit, open quote again
            digit = symbol_map[symbols[i]]
            quoted_parts.append(f'"{digit}"')
    quoted_parts.append('"')

    # Join all parts inside quotes
    quoted_expr = ''.join(quoted_parts)

    # Remove empty quotes if any (optional)
    quoted_expr = quoted_expr.replace('""', '')

    # Compose final string
    return f'{first_digit}{quoted_expr}'

def parse_mapper_expression(value, spec_name, spec_index):
    expr = str(value).strip()
    if not symbol_pattern.search(expr):
        return None  # no symbols

    mapped_expr = custom_mapper_expression(expr)
    if mapped_expr is None:
        return None

    return {str(spec_index): {spec_name: mapped_expr}}

def build_specs(row, io_type):
    specs_json = {}

    for i in range(1, 50):
        name_key = f'spec_{i}_name'
        value_key = f'spec_{i}_value_{io_type}'
        unit_key = f'spec_{i}_unit_{io_type}'

        if pd.notna(row.get(name_key)):
            value_raw = row.get(value_key, "")
            unit_raw = row.get(unit_key, "")

            value = safe_cast_value(value_raw)
            unit = str(unit_raw).strip()

            if unit.lower() in ['nan', '', 'none']:
                combined = value
            else:
                combined = f"{value} {unit}"

            specs_json[row[name_key]] = combined

    for i in range(1, 6):
        name_key = f'notes_{i}_diff'
        value_key = f'notes_{i}_value_{io_type}'
        unit_key = f'notes_{i}_unit_{io_type}'

        if pd.notna(row.get(name_key)):
            value_raw = row.get(value_key, "")
            unit_raw = row.get(unit_key, "")

            value = safe_cast_value(value_raw)
            unit = str(unit_raw).strip()

            if unit.lower() in ['nan', '', 'none']:
                combined = value
            else:
                combined = f"{value} {unit}"

            specs_json[row[name_key]] = combined

    return specs_json

def extract_mapper(row, io_type):
    mapper_json = {}

    # Collect fields related to specs and notes for this io_type
    fields_to_check = []
    # Include all spec and notes value columns for this io_type
    for col in row.index:
        if (('spec_' in col or 'notes_' in col) and f"_value_{io_type}" in col):
            fields_to_check.append(col)

    for field in fields_to_check:
        value = row.get(field)
        if pd.isna(value):
            continue
        if symbol_pattern.search(str(value)):
            # Extract index and name keys
            match_spec = re.match(r'spec_(\d+)_value', field)
            match_notes = re.match(r'notes_(\d+)_value', field)
            if match_spec:
                index = match_spec.group(1)
                name_key = f'spec_{index}_name'
                spec_name = row.get(name_key, f"unknown_{index}")
            elif match_notes:
                index = match_notes.group(1)
                name_key = f'notes_{index}_diff'
                spec_name = row.get(name_key, f"unknown_{index}")
            else:
                continue

            parsed = parse_mapper_expression(value, spec_name, index)
            if parsed:
                mapper_json.update(parsed)

    return mapper_json

# Final processed data list
processed_data = []

for _, row in df.iterrows():
    for io in ['input', 'output']:
        part_number = row.get(f'part_number_{io}')
        if pd.isna(part_number) or str(part_number).strip() == '':
            continue
        entry = {
            "PartNumber": part_number,
            "Category": row.get(f'category_{io}', ""),
            "Brand": row.get(f'brand_{io}', ""),
            "PartNumber_Type_input": row.get(f'type_{io}', ""),
            "Env_value": row.get(f'environ_value_{io}', ""),
            "Specs": build_specs(row, io),
            "mapper": extract_mapper(row, io)
        }
        processed_data.append(entry)

# Convert list of dicts to DataFrame
out_df = pd.DataFrame(processed_data)

# Save output
output_file = "transformed_output.xlsx"
with pd.ExcelWriter(output_file, engine='openpyxl', mode='w') as writer:
    out_df.to_excel(writer, sheet_name="Processed", index=False)

print("✅ Done! Transformed data written to 'Processed' sheet in:", output_file)
