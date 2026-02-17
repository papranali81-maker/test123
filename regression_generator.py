from pathlib import Path
import streamlit as st
import tempfile
import tarfile
import zipfile
import pandas as pd
from datetime import datetime
from typing import List, Optional, Tuple, Dict, Callable
# Note: switched Excel creation/modification to XlsxWriter; openpyxl imports removed
import re
from utility import _ensure_dirs, _recent_files, _validate_ccms_filename, _validate_cms_filename, _validate_cnb_filename, _save_uploaded


# ==========================================================
# Base directory (works even if you run from elsewhere)
# ==========================================================

ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / 'data' / 'regression_generator'
UPLOADED_BEFORE_CC = DATA_ROOT / 'Uploaded_files' / 'Before_Run' / 'ccms'
UPLOADED_AFTER_CC = DATA_ROOT / 'Uploaded_files' / 'After_Run' / 'ccms'
UPLOADED_BEFORE_CMS = DATA_ROOT / 'Uploaded_files' / 'Before_Run' / 'cms'
UPLOADED_AFTER_CMS = DATA_ROOT / 'Uploaded_files' / 'After_Run' / 'cms'
UPLOADED_BEFORE_CNB = DATA_ROOT / 'Uploaded_files' / 'Before_Run' / 'cnb'
UPLOADED_AFTER_CNB = DATA_ROOT / 'Uploaded_files' / 'After_Run' / 'cnb'
REPORTS_ROOT = ROOT / 'reports' / 'regression_generator'
REPORTS_CC = REPORTS_ROOT / 'ccms'
REPORTS_CMS = REPORTS_ROOT / 'cms'
REPORTS_CNB = REPORTS_ROOT / 'cnb'



# --- Function: _find_inner_out_tar ---
# Purpose: Find a nested inner 'out' tar file by keyword inside an extraction tree
# Info: helper used when dealing with nested tar archives
def _find_inner_out_tar(extract_dir: Path, keyword: str) -> Optional[Path]:
    # search for files containing keyword and ending with common tar extensions
    tar_exts = ('.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz', '.tar.xz')
    for p in extract_dir.rglob('*'):
        try:
            if p.is_file() and keyword in p.name and any(p.name.endswith(ext) for ext in tar_exts):
                return p
        except Exception:
            continue
    return None

# --- Function: read_psv_preserve_shape ---
# Purpose: Read a PSV file preserving raw rows and column counts
# Info: returns DataFrame without inferring header to avoid shifting columns
def read_psv_preserve_shape(psv_path: Path) -> pd.DataFrame:
    """Read a PSV file line-by-line preserving literal '|' splits and row shape.

    Returns a DataFrame where each row is the raw pipe-separated row and missing
    cells are filled with empty strings. If the first pipe-delimited row looks
    like a header it will be used as the DataFrame columns so Excel won't show
    default integer column names (0,1,2...). All values are returned as strings.
    """
    text = psv_path.read_text(encoding='utf-8', errors='replace')
    lines = text.splitlines()
    # skip leading empty lines
    lines = [ln for ln in lines if ln.strip() != '']
    # find first line that contains '|' to start (skip metadata before delimiter rows)
    start_idx = 0
    for i, ln in enumerate(lines):
        if '|' in ln:
            start_idx = i
            break
    rows = [ln.split('|') for ln in lines[start_idx:]]
    max_len = max((len(r) for r in rows), default=0)
    rows = [r + [''] * (max_len - len(r)) for r in rows]
    # ensure all entries are strings
    rows = [[str(cell) if cell is not None else '' for cell in row] for row in rows]
    df = pd.DataFrame(rows)
    # If there is at least one row, treat the first row as header so Excel does not
    # write numeric column names (0,1,2...). This preserves the original column
    # names when present in the PSV header line.
    if df.shape[0] >= 1:
        header_row = df.iloc[0].astype(str).tolist()
        # sanitize header names
        header_row = [h.strip() for h in header_row]
        # assign headers and drop the first row from data
        df = df.iloc[1:].copy()
        df.columns = header_row
        df = df.reset_index(drop=True)
    # Ensure all values are strings and fill NAs
    df = df.fillna('').astype(str)
    return df

# --- Function: _find_facility_header_line ---
# Purpose: Heuristic to find the header row index for facility PSV files
# Info: used by read_facility_psv_smart to skip metadata lines
def _find_facility_header_line(lines: list) -> int:
    """
    Heuristic to find header line index for facility PSV files.
    Looks for common facility column names; falls back to first '|' line.
    Returns number of lines to skip (i.e. index of header line).
    """
    header_keywords = ['facilityid', 'finalsegmentid', 'finallgdrate', 'finalead']
    for idx, ln in enumerate(lines):
        low = ln.lower()
        if '|' in ln:
            # treat pipe-delimited candidate header: check for keywords
            tokens = [t.strip().lower() for t in ln.split('|')]
            # match if any header keyword appears in the joined tokens OR appears in any individual token
            if any(k in ' '.join(tokens) for k in header_keywords) or any(k in tok for tok in tokens for k in header_keywords):
                return idx
    # fallback: first line containing '|' (use that as header)
    for idx, ln in enumerate(lines):
        if '|' in ln:
            return idx
    # nothing found, return 0
    return 0

# --- Function: _norm_col ---
# Purpose: Normalize column names for comparison and detection
# Info: lowercases and trims strings
def _norm_col(c) -> str:
    """Normalize column string for filtering unnamed columns."""
    return str(c).strip().lower()

# --- Function: read_facility_psv_smart ---
# Purpose: Read facility PSV using heuristic header detection and cleanup
# Info: returns DataFrame with trimmed column names and no unnamed cols
def read_facility_psv_smart(psv_path: Path) -> pd.DataFrame:
    """Read a facility PSV using heuristic header detection.

    Detects the header line, reads with pandas using '|' delimiter, trims
    column names, drops unnamed columns and returns all values as strings.
    """
    raw_text = psv_path.read_text(encoding='utf-8', errors='replace')
    lines = raw_text.splitlines()
    header_idx = _find_facility_header_line(lines)
    # read with pandas using header at the detected line
    df = pd.read_csv(
        psv_path,
        sep='|',
        engine='python',
        header=0,
        skiprows=header_idx,
        dtype=str,
        keep_default_na=False,
    )
    # normalize column names and drop unnamed columns
    df.columns = [str(c).strip() for c in df.columns]
    df = df.loc[:, [c for c in df.columns if not _norm_col(c).startswith('unnamed')]]
    # ensure all values are strings and fill NAs with empty string
    df = df.fillna('').astype(str)
    return df

# --- Function: _read_psv_skip_meta ---
# Purpose: Read PSV skipping any preamble lines until first '|' delimiter
# Info: robust reader to preserve text values as strings
def _read_psv_skip_meta(path: Path) -> pd.DataFrame:
    """
    Read a PSV file, skip any metadata lines before the first '|' delimiter row,
    and return a DataFrame where every cell is a string (exact text preserved).
    Missing values are converted to empty strings.
    """
    text = path.read_text(errors='ignore')
    lines = text.splitlines()
    start = 0
    for i, ln in enumerate(lines):
        if '|' in ln:
            start = i
            break
    from io import StringIO
    content = '\n'.join(lines[start:])
    # read with dtype=str to keep raw values as strings, use engine='python' for robustness
    df = pd.read_csv(StringIO(content), sep='|', dtype=str, engine='python')
    # Ensure all cells are strings and replace NaN with empty string (preserve original text)
    df = df.fillna('').astype(str)
    return df


# --- Function: _create_business_rules_df ---
# Purpose: Build DataFrame with LGD business rules for CCMS/CMS
# Info: used to annotate reports and apply validation coloring
def _create_business_rules_df():
    rules = [
        (10, '10%'), (11, '15%'), (12, '40%'), (14, '60%'), (15, '75%'), (16, '35%'),
        (17, '50%'), (18, '45%'), (19, '60%'), (3, '40%'), (1, '55%'), (2, '60%'),
        (13, '30%'), (20, '15%'), (26, '35%'), (25, '55%'), (23, '30%'), (21, '20%'),
        (22, '40%'), (30, '30%'), (65, '40%'), (64, '65%'), (34, '35%'), (33, '40%'),
        (43, '20%'), (44, '25%'), (45, '30%'), (46, '35%'), (50, '40%'), (51, '30%'),
        (52, '20%'), (53, '40%'), (54, '60%'), (60, '6%'), (61, '15%'), (63, '30%'),
        (70, '45%'), (97, 'Blended (variable)'), (98, '99%'), (99, '40%')
    ]
    df = pd.DataFrame(rules, columns=['Segment ID', 'LGD Rate'])
    df.index = range(1, len(df) + 1)
    df.index.name = 'Sr No'
    df = df.reset_index()
    return df


# --- Function: _create_business_rules_df_cnb ---
# Purpose: Build DataFrame with LGD business rules specific to CNB
# Info: used to annotate CNB facility pivot validation
def _create_business_rules_df_cnb():
    rules = [
        (102, '60%'), (104, '50%'), (106, '45%'), (107, '75%'), (110, '10%'), (111, '15%'),
        (112, '40%'), (120, '15%'), (121, '35%'), (124, '20%'), (127, '55%'), (135, '40%'),
        (143, '20%'), (144, '25%'), (145, '30%'), (146, '35%'), (147, '41%'), (148, '57%'),
        (155, '35%'), (160, '6%'), (164, '15%'), (165, '30%'), (166, '40%'), (170, '45%'),
        (197, 'Blended (variable)'), (198, '99%'), (199, '45%')
    ]
    df = pd.DataFrame(rules, columns=['Segment ID', 'LGD Rate'])
    df.index = range(1, len(df) + 1)
    df.index.name = 'Sr No'
    df = df.reset_index()
    return df


# --- Function: _resolve_required_cols ---
# Purpose: Determine the column names in a facility DataFrame for required fields
# Info: returns tuple of (facility_col, segment_col, rate_col, ead_col)
def _resolve_required_cols(df: pd.DataFrame):
    norm_map = {_norm_col(c): c for c in df.columns}

    facility_keys = ["facilityid", "facility_id", "facility id"]
    segment_keys = [
        "finalsegmentid", "final_segment_id", "final segment id",
        "segmentid", "segment_id", "segment id",
        "segment"
    ]
    rate_keys = [
        "finallgdrate", "final_lgd_rate", "final lgd rate", "lgdrate", "lgd rate"
    ]
    #Final EAD mapping keys
    ead_keys = [
        "finalead", "final_ead", "final ead", "ead"
    ]

    def pick(keys):
        for k in keys:
            nk = _norm_col(k)
            if nk in norm_map:
                return norm_map[nk]
        return None

    facility_col = pick(facility_keys)
    segment_col = pick(segment_keys)
    rate_col = pick(rate_keys)
    ead_col = pick(ead_keys)

    missing = []
    if facility_col is None:
        missing.append("FacilityID (or similar)")
    if segment_col is None:
        missing.append("FinalSegmentID/Segment ID (or similar)")
    if rate_col is None:
        missing.append("FinalLGDRate (or similar)")
    if ead_col is None:
        missing.append("FinalEAD (or similar)")

    if missing:
        raise ValueError(
            "Facility file missing required columns.\n"
            f"Missing: {missing}\n"
            f"Found columns: {list(df.columns)}"
        )

    return facility_col, segment_col, rate_col, ead_col

# python
# --- Function: _pivot_facility ---
# Purpose: Create a segment-level pivot (count, avg LGD, sum EAD) from facility DF
# Info: formats averages as percent strings and appends a Total row
def _pivot_facility(df: pd.DataFrame) -> pd.DataFrame:
    """Segment-wise count and average LGD (as whole percent string)."""
    facility_col, segment_col, rate_col, ead_col = _resolve_required_cols(df)

    work = df[[facility_col, segment_col, rate_col, ead_col]].copy()
    work[segment_col] = work[segment_col].astype(str).str.strip()

    rate = work[rate_col].astype(str).str.strip()
    rate = rate.str.replace("%", "", regex=False)
    rate = rate.str.replace(",", "", regex=False)
    rate_num = pd.to_numeric(rate, errors="coerce")

    max_rate = rate_num.max()

    if pd.notna(max_rate) and max_rate <= 1.5:
        rate_num = rate_num * 100.0

    work["__rate_num__"] = rate_num

    # EAD into Numeric
    ead = work[ead_col].astype(str).str.strip()
    ead = ead.str.replace(",", "", regex=False)
    ead_num = pd.Series(pd.to_numeric(ead, errors="coerce")).fillna(0)
    work["__ead_num__"] = ead_num

    # Pivot summary by Segment
    pt = (
        work.groupby(segment_col, dropna=False)
        .agg(
            **{
                "Count of FacilityID": (facility_col, "count"),
                "Average of FinalLGDRate": ("__rate_num__", "mean"),
                "Sum of FinalEAD": ("__ead_num__", "sum"),
            }
        )
        .reset_index()
        .rename(columns={segment_col: "Segment ID"})
    )
    # Format avg as whole percent string (same as before)
    pt["Average of FinalLGDRate"] = pt["Average of FinalLGDRate"].map(
        lambda x: "" if pd.isna(x) else f"{round(x):.0f}%"
    )

    # Keep Sum of Final EAD Numeric
    pt["Sum of FinalEAD"] = pt["Sum of FinalEAD"].map(
        lambda x: int(x) if pd.notna(x) and float(x).is_integer() else (0 if pd.isna(x) else float(x))
    )

    # Sort of Segment ID numerically
    def _seg_sort_key(v):
        try:
            return (0, int(float(str(v))))
        except Exception:
            return (1, str(v))

    pt = pt.sort_values(by="Segment ID", key=lambda s: s.map(_seg_sort_key), kind="stable").reset_index(drop=True)

    # Add Total row at end
    total_count = pd.Series(pd.to_numeric(pt["Count of FacilityID"], errors="coerce")).fillna(0).sum()
    total_ead = pd.Series(pd.to_numeric(pt["Sum of FinalEAD"], errors="coerce")).fillna(0).sum()

    total_row = {
        "Segment ID" : "Total",
        "Count of FacilityID" : int(total_count),
        "Average of FinalLGDRate" : "NA",
        "Sum of FinalEAD" : int(total_ead) if float(total_ead).is_integer() else float(total_ead),
    }

    pt = pd.concat([pt, pd.DataFrame([total_row])], ignore_index=True)

    return pt



# python
# --- Function: _write_comparison_excel ---
# Purpose: Write After/Before comparison tables and business rules to an Excel sheet
# Info: Applies formatting and calls validate_pivot_business_rules for coloring
def _write_comparison_excel(outputs: dict, out_path: Path, sheet: str = 'CCMS_Validation', module_name: str = 'CCMS'):
    """Write After/Before tables and business rules to an Excel sheet.
    Formats the sections, compares corresponding blocks and then calls
    `validate_pivot_business_rules` to apply business-rule coloring where
    applicable. Saves the workbook to `out_path`.
    """

    def _write_block(writer, df_list, startrow, startcol):
        r = startrow
        positions = {}
        for label, df in df_list:
            if df is None or df.empty:
                positions[label] = None
                continue
            df.to_excel(writer, sheet_name=sheet, startrow=r + 1, startcol=startcol, index=False)
            positions[label] = {
                'startrow': r + 1,
                'startcol': startcol,
                'nrows': df.shape[0] + 1,
                'ncols': df.shape[1]
            }
            r += df.shape[0] + 4
        return positions

    ar_order = [
        (f'After Run - Error Summary {module_name} Out File', outputs.get('ar_es')),
        (f'After Run - Summary {module_name} Out File', outputs.get('ar_sc')),
        (f'After Run - Summary Count {module_name} Out File', outputs.get('ar_scc')),
        (f'After Run - Facility {module_name} Out File - Pivot Table', outputs.get('ar_fac_pt')),
    ]
    br_order = [
        (f'Before Run - Error Summary {module_name} Out File', outputs.get('br_es')),
        (f'Before Run - Summary {module_name} Out File', outputs.get('br_sc')),
        (f'Before Run - Summary Count {module_name} Out File', outputs.get('br_scc')),
        (f'Before Run - Facility {module_name} Out File - Pivot Table', outputs.get('br_fac_pt')),
    ]

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Use XlsxWriter via pandas ExcelWriter to create and style workbook without openpyxl
    with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
        blank = pd.DataFrame()
        # create sheets and write blank frames to initialize
        blank.to_excel(writer, sheet_name=sheet, startrow=0, startcol=0, index=False)

        # helper to write blocks and record positions (positions use same semantics as before)
        def _write_block_xlsx(writer, df_list, startrow, startcol):
            r = startrow
            positions = {}
            for label, df in df_list:
                if df is None or (hasattr(df, 'empty') and df.empty):
                    positions[label] = None
                    continue
                # write dataframe such that header sits at row r+1 (to leave a label row above)
                df.to_excel(writer, sheet_name=sheet, startrow=r + 1, startcol=startcol, index=False)
                positions[label] = {
                    'startrow': r + 1,
                    'startcol': startcol,
                    'nrows': (df.shape[0] if hasattr(df, 'shape') else 0) + 1,
                    'ncols': (df.shape[1] if hasattr(df, 'shape') else 0)
                }
                r += (df.shape[0] if hasattr(df, 'shape') else 0) + 4
            return positions

        after_positions = _write_block_xlsx(writer, ar_order, startrow=2, startcol=0)
        before_positions = _write_block_xlsx(writer, br_order, startrow=2, startcol=9)
        # positions captured for later conditional-format placement

        # write business rules aligned under both After and Before blocks with same vertical start
        br_table = outputs.get('business_rules')
        if br_table is not None and not br_table.empty:
            last_after_end = 0
            for v in after_positions.values():
                if v:
                    last_after_end = max(last_after_end, v['startrow'] + v['nrows'])
            business_startrow = last_after_end + 3
            br_table.to_excel(writer, sheet_name=sheet, startrow=business_startrow, startcol=0, index=False)
            after_positions[f'Business Rule For {module_name}'] = {
                'startrow': business_startrow,
                'startcol': 0,
                'nrows': br_table.shape[0] + 1,
                'ncols': br_table.shape[1]
            }
            br_table.to_excel(writer, sheet_name=sheet, startrow=business_startrow, startcol=9, index=False)
            before_positions[f'Business Rule For {module_name}'] = {
                'startrow': business_startrow,
                'startcol': 9,
                'nrows': br_table.shape[0] + 1,
                'ncols': br_table.shape[1]
            }

        # --- New: write Facility After Vs Before sheet with raw facility tables (not pivot)
        fac_after_raw = outputs.get('ar_fac_raw')
        fac_before_raw = outputs.get('br_fac_raw')
        facility_sheet = 'Facility After Vs Before'
        blank.to_excel(writer, sheet_name=facility_sheet, startrow=0, startcol=0, index=False)
        fac_startrow = 2
        fac_after_col = 0
        if fac_after_raw is not None and not fac_after_raw.empty:
            fac_after_raw.to_excel(writer, sheet_name=facility_sheet, startrow=fac_startrow, startcol=fac_after_col, index=False)
            after_fac_pos = {
                'startrow': fac_startrow,
                'startcol': fac_after_col,
                'nrows': fac_after_raw.shape[0] + 1,
                'ncols': fac_after_raw.shape[1]
            }
        else:
            after_fac_pos = None

        fac_before_col = (fac_after_raw.shape[1] if fac_after_raw is not None and not fac_after_raw.empty else 0) + 3
        if fac_before_raw is not None and not fac_before_raw.empty:
            fac_before_raw.to_excel(writer, sheet_name=facility_sheet, startrow=fac_startrow, startcol=fac_before_col, index=False)
            before_fac_pos = {
                'startrow': fac_startrow,
                'startcol': fac_before_col,
                'nrows': fac_before_raw.shape[0] + 1,
                'ncols': fac_before_raw.shape[1]
            }
        else:
            before_fac_pos = None

        # Acquire workbook and worksheet handles for formatting
        workbook = writer.book
        ws = writer.sheets[sheet]
        fac_ws = writer.sheets.get(facility_sheet)

        # Create reusable formats
        header_fmt = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        label_fmt = workbook.add_format({'bold': True, 'align': 'left', 'valign': 'vcenter'})
        center_fmt = workbook.add_format({'align': 'center', 'valign': 'vcenter'})
        green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        yellow_fmt = workbook.add_format({'bg_color': '#FFF2CC', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        red_fmt = workbook.add_format({'bg_color': '#F4CCCC', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        normal_fmt = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})

        # --- Write table labels above each block so every table has a top label row
        try:
            for lbl, pos in after_positions.items():
                if pos:
                    try:
                        ws.write(pos['startrow'] - 1, pos['startcol'], lbl, label_fmt)
                    except Exception:
                        pass
            for lbl, pos in before_positions.items():
                if pos:
                    try:
                        ws.write(pos['startrow'] - 1, pos['startcol'], lbl, label_fmt)
                    except Exception:
                        pass
        except Exception:
            # don't fail report creation on label writing issues
            pass

        # --- Style business rules block (both After and Before) with bordered cells
        br_label = f'Business Rule For {module_name}'
        br_df = outputs.get('business_rules')
        if br_df is not None and not br_df.empty:
            # After copy
            br_pos_a = after_positions.get(br_label)
            if br_pos_a:
                sr = br_pos_a['startrow']
                sc = br_pos_a['startcol']
                # header
                for j, colname in enumerate(br_df.columns):
                    ws.write(sr, sc + j, str(colname), header_fmt)
                # rows
                for i in range(br_df.shape[0]):
                    for j in range(br_df.shape[1]):
                        ws.write(sr + 1 + i, sc + j, br_df.iat[i, j], normal_fmt)
            # Before copy
            br_pos_b = before_positions.get(br_label)
            if br_pos_b:
                sr = br_pos_b['startrow']
                sc = br_pos_b['startcol']
                for j, colname in enumerate(br_df.columns):
                    ws.write(sr, sc + j, str(colname), header_fmt)
                for i in range(br_df.shape[0]):
                    for j in range(br_df.shape[1]):
                        ws.write(sr + 1 + i, sc + j, br_df.iat[i, j], normal_fmt)

        # --- New: reapply border/header formats to every table block (After and Before)
        # This uses the original DataFrames so we preserve values and add consistent borders.
        def _style_block_from_df(df, pos):
            if df is None or pos is None:
                return
            sr = pos['startrow']
            sc = pos['startcol']
            # header
            for j, colname in enumerate(df.columns):
                try:
                    ws.write(sr, sc + j, str(colname), header_fmt)
                except Exception:
                    pass
            # rows
            for i in range(df.shape[0]):
                for j in range(df.shape[1]):
                    try:
                        ws.write(sr + 1 + i, sc + j, df.iat[i, j], normal_fmt)
                    except Exception:
                        pass

        # Apply styling to After block dataframes
        for lbl, df in ar_order:
            pos = after_positions.get(lbl)
            try:
                _style_block_from_df(df, pos)
            except Exception:
                pass

        # Apply styling to Before block dataframes
        for lbl, df in br_order:
            pos = before_positions.get(lbl)
            try:
                _style_block_from_df(df, pos)
            except Exception:
                pass

        # Top titles
        try:
            ws.write(0, 0, 'After_Run Results', label_fmt)
            ws.write(0, 9, 'Before_Run Results', label_fmt)
        except Exception:
            pass

        # Compare corresponding tables by order using XlsxWriter conditional_format (column-level, vectorized)
        def _col_idx_to_excel(col_idx: int) -> str:
            """Convert 0-based column index to Excel column letters (0 -> A)."""
            letters = ''
            n = col_idx
            while n >= 0:
                letters = chr((n % 26) + ord('A')) + letters
                n = n // 26 - 1
            return letters

        for (a_label, a_df), (b_label, b_df) in zip(ar_order, br_order):
            a_pos = after_positions.get(a_label)
            b_pos = before_positions.get(b_label)
            if not a_pos or not b_pos:
                continue
            nrows = min(a_pos['nrows'], b_pos['nrows'])
            ncols = min(a_pos['ncols'], b_pos['ncols'])
            if nrows <= 0 or ncols <= 0:
                continue
            # Excel rows/cols are 1-based for addresses
            top_row = a_pos['startrow']  # header row (0-based)
            last_row = top_row + nrows - 1
            for j in range(ncols):
                a_col = a_pos['startcol'] + j
                b_col = b_pos['startcol'] + j
                # compute Excel addresses for the top-left row of the range (use relative refs so Excel adjusts per row)
                a_col_letter = _col_idx_to_excel(a_col)
                b_col_letter = _col_idx_to_excel(b_col)
                # build formula comparing the two corresponding cells in the first row of the range
                # use relative (no $) so Excel will evaluate correctly for each row when applying to the whole range
                first_data_row = top_row + 1  # Excel header at top_row, data starts at top_row+1 (but we include header in comparison so use top_row)
                # We'll apply formula starting at header row so use row = top_row +1 in Excel terms
                excel_row_for_formula = top_row + 1
                # formula strings: e.g. =A2=K2 (no $ to keep relative row adjustment)
                formula_eq = f'={a_col_letter}{excel_row_for_formula}={b_col_letter}{excel_row_for_formula}'
                formula_neq = f'=NOT({a_col_letter}{excel_row_for_formula}={b_col_letter}{excel_row_for_formula})'
                # apply to After range
                try:
                    ws.conditional_format(top_row, a_col, last_row, a_col, {
                        'type': 'formula',
                        'criteria': formula_eq,
                        'format': green_fmt
                    })
                    ws.conditional_format(top_row, a_col, last_row, a_col, {
                        'type': 'formula',
                        'criteria': formula_neq,
                        'format': yellow_fmt
                    })
                except Exception:
                    # fallback: iterate small ranges or skip
                    pass
                # apply same formatting to Before range so both sides visually match
                try:
                    ws.conditional_format(b_pos['startrow'], b_col, b_pos['startrow'] + nrows - 1, b_col, {
                        'type': 'formula',
                        'criteria': formula_eq,
                        'format': green_fmt
                    })
                    ws.conditional_format(b_pos['startrow'], b_col, b_pos['startrow'] + nrows - 1, b_col, {
                        'type': 'formula',
                        'criteria': formula_neq,
                        'format': yellow_fmt
                    })
                except Exception:
                    pass

        # Apply business-rule coloring for facility pivot tables using DataFrames (no openpyxl)
        # Apply pivot vs business-rule validation for CCMS, CMS and CNB modules
        if module_name.upper() in ('CMS', 'CNB', 'CCMS'):
            try:
                br_df = outputs.get('business_rules')
                # build business rules map
                br_map = {}
                if br_df is not None and not br_df.empty:
                    for _, r in br_df.iterrows():
                        seg = str(r.get('Segment ID', '')).strip()
                        lgd = r.get('LGD Rate', None)
                        br_map[seg] = _parse_percent_to_float(lgd)

                def _apply_rules_to_pt(pt_df, pos):
                    """Compute mismatch for pivot avg column and write only the avg cells with red formatting for mismatches.
                    Matching cells are left unchanged (no green highlight) as requested.
                    """
                    if pt_df is None or pos is None:
                        return
                    # locate avg and segment column indices (more robust)
                    avg_col_idx = None
                    seg_col_idx = None
                    for j, col in enumerate(pt_df.columns):
                        low = str(col).lower()
                        if seg_col_idx is None and ('segment' in low or 'segment id' in low or low.strip() == 'segment id'):
                            seg_col_idx = j
                        # detect average/rate column by common tokens
                        if avg_col_idx is None and any(tok in low for tok in ('average', 'avg', 'final', 'lgd', 'rate')):
                            avg_col_idx = j
                    # fallback defaults
                    if avg_col_idx is None:
                        if pt_df.shape[1] >= 2:
                            avg_col_idx = 1
                        else:
                            avg_col_idx = 0
                    if seg_col_idx is None:
                        seg_col_idx = 0

                    # Build normalized business-rule key variants for fast lookup
                    expanded_br_map = {}
                    for k, v in br_map.items():
                        ks = str(k).strip()
                        expanded_br_map[ks] = v
                        # also try integer form where sensible
                        try:
                            kv = float(ks)
                            if float(int(kv)) == kv:
                                expanded_br_map[str(int(kv))] = v
                                expanded_br_map[str(kv)] = v
                        except Exception:
                            pass

                    # tolerance (percentage points)
                    tol = 0.5

                    # Compute and apply a single conditional-format formula across the entire Average column range of the pivot.
                    br_pos = after_positions.get(f'Business Rule For {module_name}') or before_positions.get(f'Business Rule For {module_name}')
                    if not br_pos:
                        return

                    # business rule data range (assume Segment ID in first column, LGD Rate in second column)
                    br_seg_col = _col_idx_to_excel(br_pos['startcol'] + 0)
                    br_lgd_col = _col_idx_to_excel(br_pos['startcol'] + 1)
                    br_data_start = br_pos['startrow'] + 2  # excel row (1-based) of first data row
                    br_data_end = br_pos['startrow'] + br_pos['nrows']  # excel row of last data row

                    # compute pivot data range (1-based Excel rows)
                    data_start_row = pos['startrow'] + 2  # header (0-based)+2 -> first data row (1-based)
                    data_end_row = pos['startrow'] + pos['nrows']  # last row (1-based)

                    # Build references for first data row; using relative cell references (no $) lets Excel adjust row for each cell in the range
                    seg_cell_ref = f"{_col_idx_to_excel(pos['startcol'] + seg_col_idx)}{data_start_row}"
                    avg_cell_ref = f"{_col_idx_to_excel(pos['startcol'] + avg_col_idx)}{data_start_row}"

                    br_lgd_range = f"${br_lgd_col}${br_data_start}:${br_lgd_col}${br_data_end}"
                    br_seg_range = f"${br_seg_col}${br_data_start}:${br_seg_col}${br_data_end}"

                    percent_tok = '"%"'
                    empty_tok = '""'

                    # Formula returns TRUE when mismatch (so we color red on TRUE)
                    # Note: the formula is written relative to the top-left cell of the applied range so references like C{data_start_row}
                    # will be adjusted by Excel for each row in the applied range.
                    formula = (
                        "=IFERROR(ABS(VALUE(SUBSTITUTE(" + avg_cell_ref + "," + percent_tok + "," + empty_tok + "))-"
                        "VALUE(SUBSTITUTE(INDEX(" + br_lgd_range + ",MATCH(" + seg_cell_ref + "," + br_seg_range + ",0))," + percent_tok + "," + empty_tok + ")))>0.5,TRUE)"
                    )

                    try:
                        ws.conditional_format(data_start_row - 1, pos['startcol'] + avg_col_idx, data_end_row - 1, pos['startcol'] + avg_col_idx, {
                            'type': 'formula',
                            'criteria': formula,
                            'format': red_fmt
                        })
                    except Exception:
                        # if conditional_format throws, don't fail report generation
                        pass

                # Apply to After pivot table
                pt_label = f'After Run - Facility {module_name} Out File - Pivot Table'
                pt_pos = after_positions.get(pt_label)
                pt_df = outputs.get('ar_fac_pt')
                _apply_rules_to_pt(pt_df, pt_pos)

                # Apply to Before pivot table
                pt_label_b = f'Before Run - Facility {module_name} Out File - Pivot Table'
                pt_pos_b = before_positions.get(pt_label_b)
                pt_df_b = outputs.get('br_fac_pt')
                _apply_rules_to_pt(pt_df_b, pt_pos_b)

            except Exception:
                # swallow any errors so we don't fail report creation
                pass

        # Style and compare raw facility sheet if present
        if fac_ws is not None:
            try:
                fac_ws.write(0, 0, 'After_Run Results', label_fmt)
                if before_fac_pos:
                    fac_ws.write(0, before_fac_pos['startcol'], 'Before_Run Results', label_fmt)
            except Exception:
                pass

            # write labels above raw facility tables for clarity
            try:
                if after_fac_pos:
                    fac_ws.write(after_fac_pos['startrow'] - 1, after_fac_pos['startcol'], f'After Run - Facility {module_name} Out File - Raw', label_fmt)
                if before_fac_pos:
                    fac_ws.write(before_fac_pos['startrow'] - 1, before_fac_pos['startcol'], f'Before Run - Facility {module_name} Out File - Raw', label_fmt)
            except Exception:
                pass

            def _style_fac_block_xlsx(wsh, pos):
                if not pos:
                    return
                sr = pos['startrow']
                sc = pos['startcol']
                nrows = pos['nrows']
                ncols = pos['ncols']
                # header
                try:
                    # read headers from corresponding DF if available
                    pass
                except Exception:
                    pass
                for r in range(sr, sr + nrows):
                    for c in range(sc, sc + ncols):
                        try:
                            # do not attempt to rewrite values; just ensure border/alignment by rewriting same value if needed
                            val = None
                            # Calculate DF reference row/col if possible
                            pass
                        except Exception:
                            pass

            _style_fac_block_xlsx(fac_ws, after_fac_pos)
            _style_fac_block_xlsx(fac_ws, before_fac_pos)

            # use conditional_format for facility raw comparison (column-wise)
            if after_fac_pos and before_fac_pos:
                # cast values to int to satisfy static type checkers and to be explicit
                ars = int(after_fac_pos['startrow'])
                asc = int(after_fac_pos['startcol'])
                brs = int(before_fac_pos['startrow'])
                brc = int(before_fac_pos['startcol'])
                nrows = int(min(int(after_fac_pos['nrows']), int(before_fac_pos['nrows'])))
                ncols = int(min(int(after_fac_pos['ncols']), int(before_fac_pos['ncols'])))
                if nrows > 0 and ncols > 0:
                    for j in range(ncols):
                        a_col = asc + j
                        b_col = brc + j
                        a_col_letter = _col_idx_to_excel(a_col)
                        b_col_letter = _col_idx_to_excel(b_col)
                        excel_row_for_formula = ars + 1
                        formula_eq = f'={a_col_letter}{excel_row_for_formula}={b_col_letter}{excel_row_for_formula}'
                        formula_neq = f'=NOT({a_col_letter}{excel_row_for_formula}={b_col_letter}{excel_row_for_formula})'
                        try:
                            fac_ws.conditional_format(ars, a_col, ars + nrows - 1, a_col, {
                                'type': 'formula', 'criteria': formula_eq, 'format': green_fmt
                        })
                            fac_ws.conditional_format(ars, a_col, ars + nrows - 1, a_col, {
                                'type': 'formula', 'criteria': formula_neq, 'format': yellow_fmt
                        })
                            fac_ws.conditional_format(brs, b_col, brs + nrows - 1, b_col, {
                                'type': 'formula', 'criteria': formula_eq, 'format': green_fmt
                        })
                            fac_ws.conditional_format(brs, b_col, brs + nrows - 1, b_col, {
                                'type': 'formula', 'criteria': formula_neq, 'format': yellow_fmt
                        })
                        except Exception:
                            pass

    # writer closed here; file has been saved by context manager

    # per-module zipping removed (combined zip created later by combine_excel_reports)


# --- Function: _extract_and_read_module_psvs ---
# Purpose: Extract inner out-tars from provided before/after TARs and read PSVs
# Info: returns dict of DataFrames keyed as 'ar_*' and 'br_*' for writer input
def _extract_and_read_module_psvs(before_tar: Path, after_tar: Path, inner_keywords: List[str],
                                 patterns_after: Dict[str, str], patterns_before: Dict[str, str],
                                 progress_callback: Optional[Callable[[int, str], None]] = None) -> Dict[str, Optional[pd.DataFrame]]:
    """Extract inner 'out' TARs and read matching PSV files.

    Looks for an inner out TAR matching one of `inner_keywords` inside the
    provided Before/After TARs, extracts it, finds PSV files using the
    provided glob `patterns_after`/`patterns_before`, and returns a dict of
    DataFrames keyed by 'ar_<key>' and 'br_<key>' (after/before).
    """
    outputs: Dict[str, Optional[pd.DataFrame]] = {}
    if progress_callback:
        progress_callback(1, 'Preparing temporary workspace')

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        # After_Run
        if progress_callback:
            progress_callback(5, 'Extracting After_Run container')
        with tarfile.open(after_tar) as t:
            t.extractall(path=tmp)
        inner_after = None
        for kw in inner_keywords:
            inner_after = _find_inner_out_tar(tmp, kw)
            if inner_after:
                break
        after_extract_dir = tmp / 'after_extract'
        # If an inner tar is found, extract it into after_extract_dir; otherwise
        # fall back to using the extracted tree itself when PSV files matching the requested patterns exist.
        if inner_after is not None:
            after_extract_dir.mkdir(exist_ok=True)
            with tarfile.open(inner_after) as t2:
                t2.extractall(path=after_extract_dir)
            # Also extract any additional nested inner tars that commonly hold facility files
            for extra_kw in ['esn_out', 'cms_out', 'lgd_commercial', 'commercial']:
                # find other inner tars matching the keyword and extract them into the same folder
                for p in tmp.rglob('*'):
                    try:
                        if p.is_file() and extra_kw in p.name and any(p.name.endswith(ext) for ext in ('.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz', '.tar.xz')):
                            # avoid re-extracting the same inner tar
                            if inner_after is not None and p.resolve() == inner_after.resolve():
                                continue
                            try:
                                with tarfile.open(p) as extra_t:
                                    extra_t.extractall(path=after_extract_dir)
                            except Exception:
                                # ignore extraction errors for optional inner tars
                                pass
                    except Exception:
                        continue
        else:
            # No inner tar found; check if extracted tree already contains PSV files matching requested patterns.
            found_any = False
            for pattern in patterns_after.values():
                if any(tmp.rglob(pattern)):
                    found_any = True
                    break
            if found_any:
                # use the extracted tree directly
                after_extract_dir = tmp
            else:
                raise FileNotFoundError(f'Inner out tar with keywords {inner_keywords} not found in After_Run archive')

        # search after patterns
        for logical, pattern in patterns_after.items():
            found = next(after_extract_dir.rglob(pattern), None)
            if found:
                if logical == 'fac':
                    # store facility pivot table with _pt suffix so writer finds it
                    # read raw facility table and store both raw and pivot
                    df_raw = read_facility_psv_smart(found)
                    outputs[f'ar_{logical}_raw'] = df_raw
                    outputs[f'ar_{logical}_pt'] = _pivot_facility(df_raw)
                else:
                    outputs[f'ar_{logical}'] = read_psv_preserve_shape(found)
            else:
                outputs[f'ar_{logical}'] = None

        if progress_callback:
            progress_callback(55, 'Processed After_Run files')

        # Before_Run
        if progress_callback:
            progress_callback(60, 'Extracting Before_Run container')
        with tarfile.open(before_tar) as t:
            t.extractall(path=tmp)
        inner_before = None
        for kw in inner_keywords:
            inner_before = _find_inner_out_tar(tmp, kw)
            if inner_before:
                break
        br_extract_dir = tmp / 'before_extract'
        if inner_before is not None:
            br_extract_dir.mkdir(exist_ok=True)
            with tarfile.open(inner_before) as t3:
                t3.extractall(path=br_extract_dir)
            # Also extract any additional nested inner tars that commonly hold facility files (before tar)
            for extra_kw in ['esn_out', 'cms_out', 'lgd_commercial', 'commercial']:
                for p in tmp.rglob('*'):
                    try:
                        if p.is_file() and extra_kw in p.name and any(p.name.endswith(ext) for ext in ('.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz', '.tar.xz')):
                            if inner_before is not None and p.resolve() == inner_before.resolve():
                                continue
                            try:
                                with tarfile.open(p) as extra_t:
                                    extra_t.extractall(path=br_extract_dir)
                            except Exception:
                                pass
                    except Exception:
                        continue
        else:
            found_any = False
            for pattern in patterns_before.values():
                if any(tmp.rglob(pattern)):
                    found_any = True
                    break
            if found_any:
                br_extract_dir = tmp
            else:
                raise FileNotFoundError(f'Inner out tar with keywords {inner_keywords} not found in Before_Run archive')

        # search before patterns
        for logical, pattern in patterns_before.items():
            found = next(br_extract_dir.rglob(pattern), None)
            if found:
                if logical == 'fac':
                    # read raw facility table and store both raw and pivot
                    df_raw = read_facility_psv_smart(found)
                    outputs[f'br_{logical}_raw'] = df_raw
                    outputs[f'br_{logical}_pt'] = _pivot_facility(df_raw)
                else:
                    outputs[f'br_{logical}'] = read_psv_preserve_shape(found)
            else:
                outputs[f'br_{logical}'] = None

        if progress_callback:
            progress_callback(80, 'Processed Before_Run files')

        return outputs


# --- Function: _generate_module_report ---
# Purpose: Shared generator implementing extract->read->write flow for one module
# Info: invoked by _generate_ccms/_generate_cms/_generate_cnb wrappers
def _generate_module_report(
    before_tar: Path,
    after_tar: Path,
    module_dir: str,
    inner_keywords: List[str],
    patterns: Dict[str, str],
    business_rules_provider: Optional[Callable[[], pd.DataFrame]],
    report_dir: Path,
    sheet_name: str,
    module_name: str,
    progress_callback: Optional[Callable[[int, str], None]] = None,
) -> Optional[Path]:
    """Shared generator for a single module (CCMS/CMS/CNB).

    Extracts and reads PSVs, attaches business rules (if provider given), writes
    the comparison Excel and returns the path to the generated XLSX.
    """

    if progress_callback:
        progress_callback(0, f'Starting {module_name} report generation')

    try:
        outputs = _extract_and_read_module_psvs(before_tar, after_tar, inner_keywords, patterns, patterns, progress_callback=progress_callback)
    except Exception as e:
        # bubble up so callers can report, but attach message via callback
        if progress_callback:
            progress_callback(0, f'Extraction error for {module_name}: {e}')
        raise

    # attach business rules DataFrame if provider provided
    if business_rules_provider is not None:
        try:
            outputs['business_rules'] = business_rules_provider()
        except Exception:
            outputs['business_rules'] = None
    else:
        outputs['business_rules'] = None

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_xlsx = report_dir / f'{module_name.lower()}_regression_report_{ts}.xlsx'
    if progress_callback:
        progress_callback(85, f'Writing {module_name} Excel report')
    _write_comparison_excel(outputs, out_xlsx, sheet=sheet_name, module_name=module_name)
    if progress_callback:
        progress_callback(100, f'{module_name} report generation completed')
    return out_xlsx


# --- Function: _generate_ccms_report ---
# Purpose: CCMS-specific wrapper around shared generation logic
# Info: provides file patterns, inner keywords and business rules provider
def _generate_ccms_report(before_tar: Path, after_tar: Path, progress_callback: Optional[Callable[[int,str], None]] = None) -> Optional[Path]:
    patterns = {
        'es': 'error_summary_ccms_out_*.psv',
        'sc': 'summary_ccms_out_*.psv',
        'scc': 'summary_count_ccms_out_*.psv',
        'fac': 'facility_ccms_out_*.psv',
    }
    return _generate_module_report(
        before_tar=before_tar,
        after_tar=after_tar,
        module_dir='ccms',
        inner_keywords=['ccms_out'],
        patterns=patterns,
        business_rules_provider=_create_business_rules_df,
        report_dir=REPORTS_CC,
        sheet_name='CCMS_Validation',
        module_name='CCMS',
        progress_callback=progress_callback,
    )


# --- Function: _generate_cms_report ---
# Purpose: CMS-specific wrapper around shared generation logic
# Info: handles multiple possible inner TAR name patterns
def _generate_cms_report(before_tar: Path, after_tar: Path, progress_callback: Optional[Callable[[int,str], None]] = None) -> Optional[Path]:
    patterns = {
        'es': 'error_summary_cms_out_*.psv',
        'sc': 'summary_cms_out_*.psv',
        'scc': 'summary_count_cms_out_*.psv',
        'fac': 'facility_cms_out_*.psv',
    }
    inner_kw = ['esn_out', 'lgd_commercial', 'commercial', 'cms_out']
    return _generate_module_report(
        before_tar=before_tar,
        after_tar=after_tar,
        module_dir='cms',
        inner_keywords=inner_kw,
        patterns=patterns,
        business_rules_provider=_create_business_rules_df,
        report_dir=REPORTS_CMS,
        sheet_name='CMS_Validation',
        module_name='CMS',
        progress_callback=progress_callback,
    )


# --- Function: _generate_cnb_report ---
# Purpose: CNB-specific wrapper around shared generation logic
# Info: attaches CNB business rules during generation
def _generate_cnb_report(before_tar: Path, after_tar: Path, progress_callback: Optional[Callable[[int,str], None]] = None) -> Optional[Path]:
    patterns = {
        'es': 'error_summary_cnb_out_*.psv',
        'scc': 'summary_count_cnb_out_*.psv',
        'fac': 'facility_cnb_out_*.psv',
    }
    inner_kw = ['cnb_out', 'cnb_in_out']
    return _generate_module_report(
        before_tar=before_tar,
        after_tar=after_tar,
        module_dir='cnb',
        inner_keywords=inner_kw,
        patterns=patterns,
        business_rules_provider=_create_business_rules_df_cnb,
        report_dir=REPORTS_CNB,
        sheet_name='CNB_Validation',
        module_name='CNB',
        progress_callback=progress_callback,
    )

# --- Function: _parse_percent_to_float ---
# Purpose: Parse a percent or numeric string into a float percent value
# Info: returns numeric percent, 'BLENDED' or None for unparseable/empty
def _parse_percent_to_float(s):
    """Parse a string that might be a percent or numeric and return float percent.

    Returns a float (percentage), the string 'BLENDED' for blended values,
    or None for empty/NA/unparseable values.
    """
    if s is None:
        return None
    st = str(s).strip()
    if st == "" or st.upper() == "NA":
        return None
    if re.search(r'blended', st, re.IGNORECASE):
        return 'BLENDED'
    m = re.match(r'^-?\d+(\.\d+)?%$', st)
    if m:
        try:
            return float(st.replace('%',''))
        except Exception:
            return None
    # try when number may be fractional (0.25 means 25%)
    try:
        v = float(st.replace(',',''))
        # if fractional between 0 and 1.5 treat as fraction
        if 0 <= v <= 1.5:
            return v * 100.0
        return v
    except Exception:
        return None

def _reset_regression_state():
    """Reset Streamlit session state items related to regression generation so the user can start fresh.
    Note: do NOT set file_uploader-backed keys here (Streamlit errors if you set them). Only reset simple flags and select keys.
    """
    keys_defaults = {
        'ccms_report_generation': False,
        'cms_report_generation': False,
        'cnb_report_generation': False,
        'ccms_verified_before': '',
        'ccms_verified_after': '',
        'cms_verified_before': '',
        'cms_verified_after': '',
        'cnb_verified_before': '',
        'cnb_verified_after': '',
        'before_ccms_select': '',
        'after_ccms_select': '',
        'before_cms_select': '',
        'after_cms_select': '',
        'before_cnb_select': '',
        'after_cnb_select': '',
    }
    for k, v in keys_defaults.items():
        st.session_state[k] = v


# --- Function: view ---
# Purpose: Streamlit UI for uploading/selecting TARs, verifying and generating reports
# Info: Main entrypoint used by `main.py` to render the Regression Report UI
def view():
    """Streamlit view for the Regression Report generator.
    Provides upload or select controls for CCMS, CMS, and CNB TAR files for
    Before_Run and After_Run. Allows per-module verification and has a consolidated
    "Generate Regression Report" action that runs the enabled module generators.
    A single combined ZIP download button is also provided.
    """

    # Ensure required folders(uploads, reports) exist on Root
    list_of_dirs = [UPLOADED_BEFORE_CC, UPLOADED_AFTER_CC, UPLOADED_BEFORE_CMS, UPLOADED_AFTER_CMS, UPLOADED_BEFORE_CNB,UPLOADED_AFTER_CNB, REPORTS_CC, REPORTS_CMS, REPORTS_CNB]
    _ensure_dirs(list_of_dirs)

    st.markdown("# LGD UAT Automation Solution", text_alignment="center")
    st.title('Regression Report Generator', text_alignment="center")
    st.caption('Upload or Select Before_Run & After_Run CCMS/CMS/CNB TAR files to generate Regression reports', text_alignment="center")

    # Refresh button for regression page (top-right of CCMS section)
    if st.button('Refresh', key='refresh_top'):
        _reset_regression_state()
        rerun = getattr(st, 'experimental_rerun', None)
        if callable(rerun):
            try:
                rerun()
            except Exception:
                pass

    # Initialize session keys used to remember verified status and file identity
    if 'ccms_report_generation' not in st.session_state:
        st.session_state['ccms_report_generation'] = False
    if 'cms_report_generation' not in st.session_state:
        st.session_state['cms_report_generation'] = False
    if 'cnb_report_generation' not in st.session_state:
        st.session_state['cnb_report_generation'] = False

    if 'ccms_verified_before' not in st.session_state:
        st.session_state['ccms_verified_before'] = ''
    if 'ccms_verified_after' not in st.session_state:
        st.session_state['ccms_verified_after'] = ''
    if 'cms_verified_before' not in st.session_state:
        st.session_state['cms_verified_before'] = ''
    if 'cms_verified_after' not in st.session_state:
        st.session_state['cms_verified_after'] = ''
    if 'cnb_verified_before' not in st.session_state:
        st.session_state['cnb_verified_before'] = ''
    if 'cnb_verified_after' not in st.session_state:
        st.session_state['cnb_verified_after'] = ''

    # CCMS section
    st.subheader('CCMS Section:---')
    c1, c2 = st.columns(2)
    with c1:
        before_sel = st.selectbox('Select CCMS Before_Run TAR File', options=[''] + _recent_files(UPLOADED_BEFORE_CC), key='before_ccms_select')
        before_upload = st.file_uploader('Or Upload CCMS Before_Run TAR File', type=['tar', 'gz'], key='before_ccms_upload')
        before_path_cc = None
        if before_upload is not None:
            if not _validate_ccms_filename(before_upload.name):
                st.error("Please upload 'lgd_ccms_in_out_{timestamp}.tar.gz' or 'ccms_out_{timestamp}.tar.gz'")
            else:
                before_path_cc = _save_uploaded(before_upload, UPLOADED_BEFORE_CC)
                st.success(f'Uploaded to {before_path_cc}')
        elif before_sel:
            before_path_cc = UPLOADED_BEFORE_CC / before_sel
    with c2:
        after_sel = st.selectbox('Select CCMS After_Run TAR File', options=[''] + _recent_files(UPLOADED_AFTER_CC), key='after_ccms_select')
        after_upload = st.file_uploader('Or Upload CCMS After_Run TAR File', type=['tar', 'gz'], key='after_ccms_upload')
        after_path_cc = None
        if after_upload is not None:
            if not _validate_ccms_filename(after_upload.name):
                st.error("Please upload 'lgd_ccms_in_out_{timestamp}.tar.gz' or 'ccms_out_{timestamp}.tar.gz'")
            else:
                after_path_cc = _save_uploaded(after_upload, UPLOADED_AFTER_CC)
                st.success(f'Uploaded to {after_path_cc}')
        elif after_sel:
            after_path_cc = UPLOADED_AFTER_CC / after_sel

    # Keep verification only while selected/uploaded files for CCMS remain unchanged
    current_before_cc = str(before_path_cc) if before_path_cc is not None else ''
    current_after_cc = str(after_path_cc) if after_path_cc is not None else ''
    # If files changed since last verification, clear the verified flag
    if st.session_state.get('ccms_verified_before', '') != current_before_cc or st.session_state.get('ccms_verified_after', '') != current_after_cc:
        # Only clear the generation flag if the stored verification does not match current selection
        st.session_state['ccms_report_generation'] = False

    if st.session_state.get('ccms_report_generation') and st.session_state.get('ccms_verified_before') == current_before_cc and st.session_state.get('ccms_verified_after') == current_after_cc:
        st.success('CCMS files verified')
    else:
        if before_path_cc and after_path_cc:
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
            with col5:
                if st.button('Upload File and Verify (CCMS)'):
                    st.session_state['ccms_report_generation'] = True
                    st.session_state['ccms_verified_before'] = current_before_cc
                    st.session_state['ccms_verified_after'] = current_after_cc
                    st.success('CCMS files verified')
        else:
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
            with col5:
                st.button('Upload File and Verify (CCMS)', disabled=True)

    st.markdown('---')
    # CMS section
    st.subheader('CMS Section:---')
    c1, c2 = st.columns(2)
    with c1:
        before_sel = st.selectbox('Select CMS Before_Run TAR File', options=[''] + _recent_files(UPLOADED_BEFORE_CMS), key='before_cms_select')
        before_upload = st.file_uploader('Or Upload CMS Before_Run TAR File', type=['tar', 'gz'], key='before_cms_upload')
        before_path_cms = None
        if before_upload is not None:
            if not _validate_cms_filename(before_upload.name):
                st.error("Please upload 'lgd_commercial_in_out_{timestamp}.tar.gz' or 'esn_out_{timestamp}.tar.gz' or 'cms_out_{timestamp}.tar.gz'")
            else:
                before_path_cms = _save_uploaded(before_upload, UPLOADED_BEFORE_CMS)
                st.success(f'Uploaded to {before_path_cms}')
        elif before_sel:
            before_path_cms = UPLOADED_BEFORE_CMS / before_sel
    with c2:
        after_sel = st.selectbox('Select CMS After_Run TAR File', options=[''] + _recent_files(UPLOADED_AFTER_CMS), key='after_cms_select')
        after_upload = st.file_uploader('Or Upload CMS After_Run TAR File', type=['tar', 'gz'], key='after_cms_upload')
        after_path_cms = None
        if after_upload is not None:
            if not _validate_cms_filename(after_upload.name):
                st.error("Please upload 'lgd_commercial_in_out_{timestamp}.tar.gz' or 'esn_out_{timestamp}.tar.gz' or 'cms_out_{timestamp}.tar.gz'")
            else:
                after_path_cms = _save_uploaded(after_upload, UPLOADED_AFTER_CMS)
                st.success(f'Uploaded to {after_path_cms}')
        elif after_sel:
            after_path_cms = UPLOADED_AFTER_CMS / after_sel

    # Keep verification only while selected/uploaded files for CMS remain unchanged
    current_before_cms = str(before_path_cms) if before_path_cms is not None else ''
    current_after_cms = str(after_path_cms) if after_path_cms is not None else ''
    if st.session_state.get('cms_verified_before', '') != current_before_cms or st.session_state.get('cms_verified_after', '') != current_after_cms:
        st.session_state['cms_report_generation'] = False

    if st.session_state.get('cms_report_generation') and st.session_state.get('cms_verified_before') == current_before_cms and st.session_state.get('cms_verified_after') == current_after_cms:
        st.success('CMS files verified')
    else:
        if before_path_cms and after_path_cms:
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
            with col5:
                if st.button('Upload File and Verify (CMS)'):
                    st.session_state['cms_report_generation'] = True
                    st.session_state['cms_verified_before'] = current_before_cms
                    st.session_state['cms_verified_after'] = current_after_cms
                    st.success('CMS files verified')
        else:
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
            with col5:
                st.button('Upload File and Verify (CMS)', disabled=True)

    st.markdown('---')
    # CNB section
    st.subheader('CNB Section:---')
    c1, c2 = st.columns(2)
    with c1:
        before_sel = st.selectbox('Select CNB Before_Run TAR File', options=[''] + _recent_files(UPLOADED_BEFORE_CNB), key='before_cnb_select')
        before_upload = st.file_uploader('Or Upload CNB Before_Run TAR File', type=['tar', 'gz'], key='before_cnb_upload')
        before_path_cnb = None
        if before_upload is not None:
            if not _validate_cnb_filename(before_upload.name):
                st.error("Please upload 'cnb_in_out_{timestamp}.tar.gz' or 'cnb_out_{timestamp}.tar.gz'")
            else:
                before_path_cnb = _save_uploaded(before_upload, UPLOADED_BEFORE_CNB)
                st.success(f'Uploaded to {before_path_cnb}')
        elif before_sel:
            before_path_cnb = UPLOADED_BEFORE_CNB / before_sel
    with c2:
        after_sel = st.selectbox('Select CNB After_Run TAR File', options=[''] + _recent_files(UPLOADED_AFTER_CNB), key='after_cnb_select')
        after_upload = st.file_uploader('Or Upload CNB After_Run TAR File', type=['tar', 'gz'], key='after_cnb_upload')
        after_path_cnb = None
        if after_upload is not None:
            if not _validate_cnb_filename(after_upload.name):
                st.error("Please upload 'cnb_in_out_{timestamp}.tar.gz' or 'cnb_out_{timestamp}.tar.gz'")
            else:
                after_path_cnb = _save_uploaded(after_upload, UPLOADED_AFTER_CNB)
                st.success(f'Uploaded to {after_path_cnb}')
        elif after_sel:
            after_path_cnb = UPLOADED_AFTER_CNB / after_sel

    # Keep verification only while selected/uploaded files for CNB remain unchanged
    current_before_cnb = str(before_path_cnb) if before_path_cnb is not None else ''
    current_after_cnb = str(after_path_cnb) if after_path_cnb is not None else ''
    if st.session_state.get('cnb_verified_before', '') != current_before_cnb or st.session_state.get('cnb_verified_after', '') != current_after_cnb:
        st.session_state['cnb_report_generation'] = False

    if st.session_state.get('cnb_report_generation') and st.session_state.get('cnb_verified_before') == current_before_cnb and st.session_state.get('cnb_verified_after') == current_after_cnb:
        st.success('CNB files verified')
    else:
        if before_path_cnb and after_path_cnb:
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
            with col5:
                if st.button('Upload File and Verify (CNB)'):
                    st.session_state['cnb_report_generation'] = True
                    st.session_state['cnb_verified_before'] = current_before_cnb
                    st.session_state['cnb_verified_after'] = current_after_cnb
                    st.success('CNB files verified')
        else:
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
            with col5:
                st.button('Upload File and Verify (CNB)', disabled=True)

    st.markdown('---')
    # Generate consolidated regression report for enabled modules
    any_ready = st.session_state.get('ccms_report_generation') or st.session_state.get('cms_report_generation') or st.session_state.get('cnb_report_generation')
    if any_ready:
        if st.button('Generate Regression Report'):
            st.info('Starting generation for enabled modules...')
            # Setup UI progress
            progress_bar = st.progress(0)
            status_box = st.empty()
            def ui_cb(pct, msg):
                try:
                    progress_bar.progress(min(max(int(pct), 0), 100))
                except Exception:
                    pass
                status_box.info(msg)

            generated = {}
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            # generate per module if flagged
            # Prepare per-module progress UI for modules that will be generated
            modules_to_run = []
            if st.session_state.get('ccms_report_generation'):
                modules_to_run.append('ccms')
            if st.session_state.get('cms_report_generation'):
                modules_to_run.append('cms')
            if st.session_state.get('cnb_report_generation'):
                modules_to_run.append('cnb')

            progress_widgets = {}
            for mod in modules_to_run:
                container = st.container()
                container.markdown(f'**{mod.upper()} Report Progress:**')
                pb = container.progress(0)
                status = container.empty()
                progress_widgets[mod] = (pb, status)

            # helper to create per-module callbacks
            def make_cb(mod_name):
                def cb(pct, msg):
                    try:
                        pair = progress_widgets.get(mod_name)
                        if pair:
                            pb, stbox = pair
                        else:
                            pb = stbox = None
                        if pb is not None:
                            try:
                                pb.progress(min(max(int(pct), 0), 100))
                            except Exception:
                                pass
                        if stbox is not None:
                            try:
                                stbox.info(msg)
                            except Exception:
                                pass
                    except Exception:
                        pass
                return cb

            # run modules sequentially with their own callbacks
            if 'ccms' in progress_widgets:
                try:
                    out_ccms_xlsx = _generate_ccms_report(before_path_cc, after_path_cc, progress_callback=make_cb('ccms'))
                    if out_ccms_xlsx and out_ccms_xlsx.exists():
                        generated['ccms'] = out_ccms_xlsx
                except Exception as e:
                    st.error(f'CCMS generation error: {e}')
            if 'cms' in progress_widgets:
                try:
                    out_cms_xlsx = _generate_cms_report(before_path_cms, after_path_cms, progress_callback=make_cb('cms'))
                    if out_cms_xlsx and out_cms_xlsx.exists():
                        generated['cms'] = out_cms_xlsx
                except Exception as e:
                    st.error(f'CMS generation error: {e}')
            if 'cnb' in progress_widgets:
                try:
                    out_cnb_xlsx = _generate_cnb_report(before_path_cnb, after_path_cnb, progress_callback=make_cb('cnb'))
                    if out_cnb_xlsx and out_cnb_xlsx.exists():
                        generated['cnb'] = out_cnb_xlsx
                except Exception as e:
                    st.error(f'CNB generation error: {e}')

            # combine xlsx into one zip
            if generated:
                final_zip = REPORTS_ROOT / f'regression_report_{ts}.zip'
                zip_path, missing = combine_excel_reports(list(generated.values()), final_zip)
                if missing:
                    st.warning(f"Missing expected reports: {', '.join(missing)}")
                if zip_path:
                    # Expose a single download button; capture click to reset flags and show success
                    with open(zip_path, 'rb') as f:
                        data = f.read()
                    # store last zip path in session for later logic
                    st.session_state['last_regression_zip'] = str(zip_path)
                    downloaded_flag = st.session_state.get('zip_downloaded', False)
                    clicked = st.download_button('Download Regression Reports ZIP', data, file_name=zip_path.name, mime='application/zip', disabled=downloaded_flag)

                    if clicked:
                        # mark as downloaded and reset module generation flags
                        st.session_state['zip_downloaded'] = True
                        st.session_state['ccms_report_generation'] = False
                        st.session_state['cms_report_generation'] = False
                        st.session_state['cnb_report_generation'] = False
                        st.success(f'{zip_path.name} file downloaded successfully')
                else:
                    st.error('Failed to create combined zip')


# --- Function: combine_excel_reports ---
# Purpose: Zip multiple Excel files into a single combined ZIP file
# Info: Returns (zip_path, missing_files_list) so UI can warn about missing pieces
def combine_excel_reports(files: List[Path], out_zip: Path) -> Tuple[Optional[Path], List[str]]:
    """Combine existing Excel files from `files` into a single zip at `out_zip`.
    Returns (zip_path or None if nothing to zip, list_of_missing_filenames).
    """
    existing = []
    missing = []
    for p in files:
        if p and p.exists():
            existing.append(p)
        else:
            missing.append(p.name if isinstance(p, Path) else str(p))

    if not existing:
        return None, missing

    out_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_zip, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for p in existing:
            zf.write(p, arcname=p.name)
    return out_zip, missing


# --- Function: streamlit_combined_reports_download ---
# Purpose: Helper that creates combined zip and exposes a Streamlit download button
# Info: Convenience wrapper used by the UI to show missing-file warnings
def streamlit_combined_reports_download(report_paths: Dict[str, Path], zip_output: Path):
    """
    Given a dict of report label -> Path (e.g. {'ccms': Path(...), 'cms': Path(...), 'cnb': Path(...)}),
    create a combined zip and expose a single Streamlit download button.
    Shows a warning if some expected Excel files are not generated.
    """
    files = [p for p in report_paths.values() if p]
    zip_path, missing = combine_excel_reports(files, zip_output)

    if missing:
        st.warning(f"These {', '.join(missing)} excel is not generated.")

    if not zip_path or not zip_path.exists():
        st.info("No reports available for download.")
        return

    # Single download button for the combined zip
    with open(zip_path, "rb") as f:
        data = f.read()
    st.download_button(
        label="Download Regression Reports ZIP",
        data=data,
        file_name=zip_path.name,
        mime="application/zip"
    )

