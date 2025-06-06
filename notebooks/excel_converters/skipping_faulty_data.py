import pandas as pd
import re
import os
from glob import glob

# === CONFIG ===
input_folder = r"C:\Users\user\Downloads\All_Timing_Belt_files"      # Folder with all your input files
output_folder = r"C:\Users\user\Downloads\transformed_folder_after_removing_faulty_data"
os.makedirs(output_folder, exist_ok=True)
BATCH_SIZE = 10000

# === SYMBOL MAP ===
symbol_map = {
    '①': '1', '②': '2', '③': '3', '④': '4', '⑤': '5',
    '⑥': '6', '⑦': '7', '⑧': '8', '⑨': '9', '⑩': '10'
}
symbol_pattern = re.compile('|'.join(re.escape(sym) for sym in symbol_map))

# === FUNCTIONS ===

def safe_cast_value(val):
    val_str = str(val).strip()
    if val_str.lower() in ['nan', '', 'none']:
        return ""
    try:
        if '.' in val_str:
            float_val = float(val_str)
            return int(float_val) if float_val.is_integer() else float_val
        elif val_str.isdigit():
            return int(val_str)
        return val_str
    except:
        return val_str

def format_number(val):
    try:
        f = float(val)
        return str(int(f)) if f.is_integer() else str(f)
    except:
        return str(val).strip()

def custom_mapper_expression(expr):
    expr = str(expr).strip()
    if not symbol_pattern.search(expr):
        return None
    match = symbol_pattern.search(expr)
    if not match:
        return None

    first_symbol = match.group(0)
    first_digit = symbol_map[first_symbol]
    remaining_expr = expr[:match.start()] + expr[match.end():]
    parts = symbol_pattern.split(remaining_expr)
    symbols = symbol_pattern.findall(remaining_expr)

    quoted_parts = ['"']
    for i, part in enumerate(parts):
        quoted_parts.append(part)
        if i < len(symbols):
            digit = symbol_map[symbols[i]]
            quoted_parts.append(f'"{digit}"')
    quoted_parts.append('"')
    quoted_expr = ''.join(quoted_parts).replace('""', '')

    return f'{first_digit}{quoted_expr}'

def parse_mapper_expression(value, spec_name, spec_index):
    expr = str(value).strip()
    if not symbol_pattern.search(expr):
        return None
    mapped_expr = custom_mapper_expression(expr)
    return {str(spec_index): {spec_name: mapped_expr}} if mapped_expr else None

def replace_circled_params(val, row, io_type, append_units=True):
    val = str(val)
    for i in range(1, 11):
        symbol = chr(9311 + i)  # ① starts at Unicode 9312 (9311+1)
        irregular_key = f'params_{i}_irregular value_{io_type}'
        unit_key = f'params_{i}_unit_{io_type}'
        min_key = f'params_{i}_rule value_min_{io_type}'
        max_key = f'params_{i}_rule value_max_{io_type}'
        range_key = f'params_{i}_rule value_range_{io_type}'

        irregular = str(row.get(irregular_key, "")).strip()
        unit = str(row.get(unit_key, "")).strip()
        min_val = row.get(min_key, "")
        max_val = row.get(max_key, "")
        range_val = row.get(range_key, "")

        if unit.lower() in ['nan', '', 'none']:
            unit = ''

        replacement = ""
        if irregular and irregular.lower() != 'nan':
            replacement = f'[{irregular}]'
            if append_units:
                replacement += unit
        elif all(pd.notna(x) and str(x).strip() != '' for x in [min_val, max_val, range_val]):
            replacement = f'[{format_number(min_val)}-{format_number(max_val)}/{format_number(range_val)}]'
            if append_units:
                replacement += unit

        if replacement:
            val = val.replace(symbol, replacement)
    return val


def build_specs(row, io_type):
    specs_json = {}
    for i in range(1, 50):
        name_key = f'spec_{i}_name'
        value_key = f'spec_{i}_value_{io_type}'
        unit_key = f'spec_{i}_unit_{io_type}'

        if pd.notna(row.get(name_key)):
            value_raw = row.get(value_key, "")
            value_raw = replace_circled_params(value_raw, row, io_type)
            unit_raw = row.get(unit_key, "")
            unit = str(unit_raw).strip()

            if unit.lower() in ['nan', '', 'none']:
                combined = safe_cast_value(value_raw)
            else:
                combined = f"{safe_cast_value(value_raw)} {unit}"

            specs_json[row[name_key]] = combined

    for i in range(1, 6):
        name_key = f'notes_{i}_diff'
        value_key = f'notes_{i}_value_{io_type}'
        unit_key = f'notes_{i}_unit_{io_type}'

        if pd.notna(row.get(name_key)):
            value_raw = row.get(value_key, "")
            value_raw = replace_circled_params(value_raw, row, io_type)
            unit_raw = row.get(unit_key, "")
            unit = str(unit_raw).strip()

            if unit.lower() in ['nan', '', 'none']:
                combined = safe_cast_value(value_raw)
            else:
                combined = f"{safe_cast_value(value_raw)} {unit}"

            specs_json[row[name_key]] = combined

    return specs_json

def extract_mapper(row, io_type):
    mapper_json = {}
    for col in row.index:
        if (('spec_' in col or 'notes_' in col) and f"_value_{io_type}" in col):
            value = row.get(col)
            if pd.isna(value):
                continue
            if symbol_pattern.search(str(value)):
                match_spec = re.match(r'spec_(\d+)_value', col)
                match_notes = re.match(r'notes_(\d+)_value', col)
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

def write_batch(batch_data, batch_number):
    if not batch_data:
        return
    out_df = pd.DataFrame(batch_data)
    output_path = os.path.join(output_folder, f"transformed_output_part_{batch_number}.xlsx")
    out_df.to_excel(output_path, index=False, sheet_name="Processed")
    print(f"✅ Wrote batch {batch_number}: {output_path}")

# === NEW FUNCTION: Symbol + Operator Check ===
def should_skip_io_side(row, io_type):
    symbols_regex = r'[①-⑩][\*\+\-/]'
    for col in row.index:
        if (f"_value_{io_type}" in col and ("spec_" in col or "notes_" in col)):
            val = str(row.get(col, "")).strip()
            if re.search(symbols_regex, val):
                return True
    return False

# === MAIN PROCESSING ===
batch = []
batch_count = 1

for file in glob(os.path.join(input_folder, "*.xlsx")):
    print(f"Processing: {os.path.basename(file)}")
    try:
        df = pd.read_excel(file, header=6, engine='openpyxl')
        for _, row in df.iterrows():
            skip_input = should_skip_io_side(row, "input")
            skip_output = should_skip_io_side(row, "output")

            if skip_input and skip_output:
                continue  # Skip entire row

            for io in ['input', 'output']:
                if (io == 'input' and skip_input) or (io == 'output' and skip_output):
                    continue  # Skip this side only

                part_number = row.get(f'part_number_{io}')
                if pd.isna(part_number) or str(part_number).strip() == '':
                    continue
                # Important: append_units=False here
                part_number_cleaned = replace_circled_params(part_number, row, io, append_units=False)

                entry = {
                    "PartNumber": part_number_cleaned,
                    "Category": row.get(f'category_{io}', ""),
                    "Brand": row.get(f'brand_{io}', ""),
                    "PartNumber_Type_input": row.get(f'type_{io}', ""),
                    "Env_value": row.get(f'environ_value_{io}', ""),
                    "Specs": build_specs(row, io),
                    "mapper": extract_mapper(row, io)
                }
                batch.append(entry)


                if len(batch) >= BATCH_SIZE:
                    write_batch(batch, batch_count)
                    batch = []
                    batch_count += 1
    except Exception as e:
        print(f"⚠️ Failed to process {file}: {e}")

# Write any remaining data
if batch:
    write_batch(batch, batch_count)

print("🎉 All files processed and batches saved.")
