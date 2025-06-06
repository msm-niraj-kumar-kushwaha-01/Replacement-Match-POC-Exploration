import pandas as pd
import json

# Load the Excel file with headers on the 7th row (i.e., header=6)
input_file = r"C:\Users\user\Downloads\All_Timing_Belt_files\20241022_GATESUNITTA_MISUMI_TimingBeltConveyor_EndlessType_01_Jyoti_Umar_complete.xlsx"
df = pd.read_excel(input_file, header=6)

# Create a copy to work on replacements
df_replaced = df.copy()

# Symbol to parameter index mapping
symbols = ['①', '②', '③', '④', '⑤', '⑥', '⑦', '⑧', '⑨', '⑩']
symbol_param_map = {symbol: str(i + 1) for i, symbol in enumerate(symbols)}

# Relevant columns to perform replacements in
target_cols = [
    col for col in df.columns
    if (
        col in ["part_number_input", "part_number_output"]
        or (col.startswith("spec_") and "_value" in col)
        or (col.startswith("notes_") and "_value" in col)
    )
]

# Helper to format number cleanly
def clean_number(n):
    try:
        n_float = float(n)
        return int(n_float) if n_float.is_integer() else n_float
    except:
        return n

# Symbol replacement function
def replace_symbol(val, symbol, row, io, param_idx, include_unit):
    irreg = row.get(f'params_{param_idx}_irregular value_{io}', '')
    rmin = row.get(f'params_{param_idx}_rule value_min_{io}', '')
    rmax = row.get(f'params_{param_idx}_rule value_max_{io}', '')
    rrange = row.get(f'params_{param_idx}_rule value_range_{io}', '')
    unit = row.get(f'params_{param_idx}_unit_{io}', '')

    if pd.isna(val) or symbol not in str(val):
        return val

    unit_str = f"{unit}" if pd.notna(unit) and str(unit).strip() and include_unit else ""

    if pd.notna(irreg) and str(irreg).strip():
        return str(val).replace(symbol, f"[{irreg}]{unit_str}")
    elif all(pd.notna(x) and str(x).strip() for x in [rmin, rmax, rrange]):
        rmin_fmt = clean_number(rmin)
        rmax_fmt = clean_number(rmax)
        rrange_fmt = clean_number(rrange)
        return str(val).replace(symbol, f"[{rmin_fmt}-{rmax_fmt}/{rrange_fmt}]{unit_str}")
    else:
        return val

# Apply replacements
for col in target_cols:
    io_type = 'input' if '_input' in col else 'output' if '_output' in col else None
    if not io_type:
        continue

    include_unit = col not in ["part_number_input", "part_number_output"]

    for symbol, param_idx in symbol_param_map.items():
        df_replaced[col] = df_replaced.apply(
            lambda row: replace_symbol(row[col], symbol, row, io_type, param_idx, include_unit),
            axis=1
        )

# Now build Sheet2 from transformed data
records = []

def get_clean_value(row, col):
    val = row.get(col, "")
    return "" if pd.isna(val) else str(val).strip()

def combine_value_unit(val, unit):
    val = str(val).strip()
    unit = str(unit).strip()
    if not val or val.lower() == 'nan':
        return ""
    return f"{val}{unit}" if unit and unit.lower() != "nan" else val

for _, row in df_replaced.iterrows():
    for io_type in ["input", "output"]:
        record = {}

        record["PartNumber"] = get_clean_value(row, f"part_number_{io_type}")
        if not record["PartNumber"]:
            continue

        record["Category"] = get_clean_value(row, f"category_{io_type}")
        record["Brand"] = get_clean_value(row, f"brand_{io_type}")
        record["PartNumber_Type_input"] = get_clean_value(row, f"type_{io_type}")
        record["Env_value"] = get_clean_value(row, f"environ_value_{io_type}")

        specs_json = {}
        count = 1

        for i in range(1, 50):
            name = get_clean_value(row, f"spec_{i}_name")
            if not name:
                continue
            val = get_clean_value(row, f"spec_{i}_value_{io_type}")
            unit = get_clean_value(row, f"spec_{i}_unit_{io_type}")
            combined = combine_value_unit(val, unit)
            if combined:
                specs_json[str(count)] = {name: combined}
                count += 1

        for i in range(1, 6):
            name = get_clean_value(row, f"notes_{i}_diff")
            if not name:
                continue
            val = get_clean_value(row, f"notes_{i}_value_{io_type}")
            unit = get_clean_value(row, f"notes_{i}_unit_{io_type}")
            combined = combine_value_unit(val, unit)
            if combined:
                specs_json[str(count)] = {name: combined}
                count += 1

        record["Specs mapper"] = json.dumps(specs_json, ensure_ascii=False)
        records.append(record)

df_sheet2 = pd.DataFrame(records)

# Save both sheets
output_file = r"C:\Users\user\Downloads\final_output_with_sheet2.xlsx"
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    df_replaced.to_excel(writer, sheet_name='Sheet1', index=False)
    df_sheet2.to_excel(writer, sheet_name='Sheet2', index=False)

print("✅ File saved successfully at:", output_file)
