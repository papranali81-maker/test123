import shutil
import tarfile
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Dict

import duckdb
import fnmatch
import pandas as pd
import streamlit as st
import xlsxwriter

from utility import _ensure_dirs, _recent_files, _validate_ccms_filename, _validate_cms_filename, \
    _validate_cnb_filename, _save_uploaded



# ==========================================================
# Base directory (works even if you run from elsewhere)
# ==========================================================

ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / 'data' / 'consolidated_generator'
UPLOADED_CC = DATA_ROOT / 'Uploaded_files' / 'ccms'
UPLOADED_CMS = DATA_ROOT / 'Uploaded_files' / 'cms'
UPLOADED_CNB = DATA_ROOT / 'Uploaded_files' / 'cnb'
EXTRACTED = DATA_ROOT / 'Extracted_psv_files'
EXTRACTED_CC = DATA_ROOT / 'Extracted_psv_files'/ 'ccms'
EXTRACTED_CMS = DATA_ROOT / 'Extracted_psv_files'/ 'cms'
EXTRACTED_CNB = DATA_ROOT / 'Extracted_psv_files'/ 'cnb'
REPORTS_ROOT = ROOT / 'reports' / 'consolidated_generator'
REPORTS_CC = REPORTS_ROOT / 'ccms'
REPORTS_CMS = REPORTS_ROOT / 'cms'
REPORTS_CNB = REPORTS_ROOT / 'cnb'


###############################################################################

# --- New helper imports from regression_generator to reuse PSV readers and helpers ---
try:
    from regression_generator import _find_inner_out_tar, read_psv_preserve_shape, _read_psv_skip_meta, read_facility_psv_smart
except Exception:
    # graceful fallback if import fails in some contexts; we'll implement simple readers below if needed
    _find_inner_out_tar = None


# --- New helper: map module -> extracted folder ---
_module_to_extracted = {
    'ccms': EXTRACTED_CC,
    'cms': EXTRACTED_CMS,
    'cnb': EXTRACTED_CNB,
}

required_files_for_ccms = [
    'Facility_ccms_in_{*}.psv',
    'Facility_ccms_out_{*}.psv',
    'Collateral_ccms_in_{*}.psv',
    'Collateral_ccms_out_{*}.psv',
    'Borrower_ccms_in_{*}.psv',
    'Borrower_ccms_out_{*}.psv',
]

required_files_for_cms = [
    'Facility_esn_out_{*}.psv',
    'Facility_cms_out_{*}.psv',
    'Collateral_esn_out_{*}.psv',
    'Collateral_cms_out_{*}.psv',
    'Guarantee_esn_out_{*}.psv',
    'Borrower_cms_out_{*}.psv',
    'Borrower_esn_out_{*}.psv',
    'Pledge_esn_out_{*}.psv',
    'Singlename_cms_out_{*}.psv',
]

required_files_for_cnb = [
    'Facility_cnb_in_{*}.psv',
    'Facility_cnb_out_{*}.psv',
    'Collateral_cnb_in_{*}.psv',
    'Collateral_cnb_out_{*}.psv',
    'Guarantee_cnb_in_{*}.psv',
    'Borrower_cnb_in_{*}.psv',
    'Borrower_cnb_out_{*}.psv',
    'Pledge_cnb_in_{*}.psv',
]
############################################
# --- Embedded SQL queries ---
# CCMS SQL Without CRE
ccms_sql_query_without_cre = """
SELECT 
    Facility_ccms_in_{*}.deposit, Facility_ccms_in_{*}.SharedLimitTXID, Facility_ccms_in_{*}.UCCIndicator,
    Facility_ccms_in_{*}.CREType, Facility_ccms_in_{*}.ABLIndicator,Facility_ccms_in_{*}.REPortfolioType,
    Facility_ccms_in_{*}.EnrollList,Collateral_ccms_out_{*}.CollateralID,Collateral_ccms_out_{*}.SegmentID,Collateral_ccms_out_{*}.ActualLTV,Collateral_ccms_out_{*}.SegmentIDUpdateDate,Collateral_ccms_out_{*}.CollateralCountryCode,Collateral_ccms_out_{*}.LGDProductPurposeCode,Collateral_ccms_out_{*}.AllocationValue,Collateral_ccms_out_{*}.UCCIndicator,Collateral_ccms_out_{*}.LevelErrorList,Collateral_ccms_in_{*}.GuaranteeID,Collateral_ccms_in_{*}.PledgedType,
    Collateral_ccms_out_{*}.CollateralTypeCd,Collateral_ccms_in_{*}.CollateralSubTypeCd,Collateral_ccms_in_{*}.CollateralProvinceStateCd,Collateral_ccms_in_{*}.CollateralMarketAmountCAD,Collateral_ccms_in_{*}.CollateralBookAmountCAD,Collateral_ccms_out_{*}.CollateralAppraisedAmountCAD,Collateral_ccms_in_{*}.PriorLiensAmountCAD,Collateral_ccms_in_{*}.ChargeAmountCAD,Collateral_ccms_in_{*}.EligibleCollateralAmountCAD,Collateral_ccms_in_{*}.CollateralAssignmentTypeCd,Collateral_ccms_out_{*}.LGDProductPurposeCode,Collateral_ccms_out_{*}.ProvidedByCustomerId,Collateral_ccms_out_{*}.UCCIndicator,Collateral_ccms_out_{*}.ValuationMethod,
    Collateral_ccms_out_{*}.ValuationDate,Collateral_ccms_out_{*}.UnderWrittenValue,Collateral_ccms_out_{*}.CROCOComplaintIndicator,Collateral_ccms_out_{*}.ErrorList,Guarantee_ccms_in_{*}.GuaranteeID,
    Guarantee_ccms_in_{*}.GuaranteeType,Guarantee_ccms_in_{*}.GuaranteeSubType,Guarantee_ccms_in_{*}.GBRR,Guarantee_ccms_in_{*}.GuaranteeDocumentId,Guarantee_ccms_in_{*}.GuaranteeIndicators,Guarantee_ccms_in_{*}.GuaranteeAssignmentType,Guarantee_ccms_in_{*}.GuaranteeCAD,Guarantee_ccms_in_{*}.GuarantorType,
    Guarantee_ccms_in_{*}.GuarantorSICCode,Guarantee_ccms_in_{*}.GuarantorSectorTypeCode,Guarantee_ccms_in_{*}.GuarantorBSC,Guarantee_ccms_in_{*}.GuarantorCountyCode,Guarantee_ccms_in_{*}.GuarantorCTS
FROM 
    Facility_ccms_out_{*}
    LEFT JOIN Facility_ccms_in_{*} 
        ON Facility_ccms_out_{*}.FacilityID = Facility_ccms_in_{*}.FacilityID
       AND Facility_ccms_out_{*}.ccmsBorrowerCTS = Facility_ccms_in_{*}.ccmsBorrowerCTS
    LEFT JOIN Collateral_ccms_out_{*} 
        ON Facility_ccms_out_{*}.FacilityID = Collateral_ccms_out_{*}.FacilityID
    LEFT JOIN Collateral_ccms_in_{*} 
        ON Collateral_ccms_out_{*}.CollateralID = Collateral_ccms_in_{*}.CollateralID
    LEFT JOIN Borrower_ccms_in_{*} 
        ON Collateral_ccms_out_{*}.ccmsBorrowerCTS = Borrower_ccms_in_{*}.ccmsBorrowerCTS
    LEFT JOIN Pledge_ccms_in_{*} 
        ON Collateral_ccms_out_{*}.CollateralID = Pledge_ccms_in_{*}.CollateralID
    LEFT JOIN Guarantee_ccms_in_{*} 
        ON Pledge_ccms_in_{*}.GuaranteeID = Guarantee_ccms_in_{*}.GuaranteeID
"""

# CCMS SQL With CRE
ccms_sql_query_with_cre = """
SELECT 
    Facility_ccms_in_{*}.deposit, Facility_ccms_in_{*}.SharedLimitTXID, Facility_ccms_in_{*}.UCCIndicator,
    Facility_ccms_in_{*}.CREType, Facility_ccms_in_{*}.ABLIndicator,Facility_ccms_in_{*}.REPortfolioType,
    Facility_ccms_in_{*}.EnrollList,Collateral_ccms_out_{*}.CollateralID,Collateral_ccms_out_{*}.SegmentID,Collateral_ccms_out_{*}.ActualLTV,Collateral_ccms_out_{*}.SegmentIDUpdateDate,Collateral_ccms_out_{*}.CollateralCountryCode,Collateral_ccms_out_{*}.LGDProductPurposeCode,Collateral_ccms_out_{*}.AllocationValue,Collateral_ccms_out_{*}.UCCIndicator,Collateral_ccms_out_{*}.LevelErrorList,Collateral_ccms_in_{*}.GuaranteeID,Collateral_ccms_in_{*}.PledgedType,
    Collateral_ccms_out_{*}.CollateralTypeCd,Collateral_ccms_in_{*}.CollateralSubTypeCd,Collateral_ccms_in_{*}.CollateralProvinceStateCd,Collateral_ccms_in_{*}.CollateralMarketAmountCAD,Collateral_ccms_in_{*}.CollateralBookAmountCAD,Collateral_ccms_out_{*}.CollateralAppraisedAmountCAD,Collateral_ccms_in_{*}.PriorLiensAmountCAD,Collateral_ccms_in_{*}.ChargeAmountCAD,Collateral_ccms_in_{*}.EligibleCollateralAmountCAD,Collateral_ccms_in_{*}.CollateralAssignmentTypeCd,Collateral_ccms_out_{*}.LGDProductPurposeCode,Collateral_ccms_out_{*}.ProvidedByCustomerId,Collateral_ccms_out_{*}.UCCIndicator,Collateral_ccms_out_{*}.ValuationMethod,
    Collateral_ccms_out_{*}.ValuationDate,Collateral_ccms_out_{*}.UnderWrittenValue,Collateral_ccms_out_{*}.CROCOComplaintIndicator,Collateral_ccms_out_{*}.ErrorList,Guarantee_ccms_in_{*}.GuaranteeID,
    Guarantee_ccms_in_{*}.GuaranteeType,Guarantee_ccms_in_{*}.GuaranteeSubType,Guarantee_ccms_in_{*}.GBRR,Guarantee_ccms_in_{*}.GuaranteeDocumentId,Guarantee_ccms_in_{*}.GuaranteeIndicators,Guarantee_ccms_in_{*}.GuaranteeAssignmentType,Guarantee_ccms_in_{*}.GuaranteeCAD,Guarantee_ccms_in_{*}.GuarantorType,
    Guarantee_ccms_in_{*}.GuarantorSICCode,Guarantee_ccms_in_{*}.GuarantorSectorTypeCode,Guarantee_ccms_in_{*}.GuarantorBSC,Guarantee_ccms_in_{*}.GuarantorCountyCode,Guarantee_ccms_in_{*}.GuarantorCTS
FROM 
    Facility_ccms_out_{*}
    LEFT JOIN Facility_ccms_in_{*} 
        ON Facility_ccms_out_{*}.FacilityID = Facility_ccms_in_{*}.FacilityID
       AND Facility_ccms_out_{*}.ccmsBorrowerCTS = Facility_ccms_in_{*}.ccmsBorrowerCTS
    LEFT JOIN Collateral_ccms_out_{*} 
        ON Facility_ccms_out_{*}.FacilityID = Collateral_ccms_out_{*}.FacilityID
    LEFT JOIN Collateral_ccms_in_{*} 
        ON Collateral_ccms_out_{*}.CollateralID = Collateral_ccms_in_{*}.CollateralID
    LEFT JOIN Borrower_ccms_in_{*} 
        ON Collateral_ccms_out_{*}.ccmsBorrowerCTS = Borrower_ccms_in_{*}.ccmsBorrowerCTS
    LEFT JOIN Pledge_ccms_in_{*} 
        ON Collateral_ccms_out_{*}.CollateralID = Pledge_ccms_in_{*}.CollateralID
    LEFT JOIN Guarantee_ccms_in_{*} 
        ON Pledge_ccms_in_{*}.GuaranteeID = Guarantee_ccms_in_{*}.GuaranteeID
"""

# CMS SQL
cms_sql_query = """
SELECT 
    Facility_cms_in_{*}.deposit, Facility_cms_in_{*}.SharedLimitTXID, Facility_cms_in_{*}.UCCIndicator,
    Facility_cms_in_{*}.CREType, Facility_cms_in_{*}.ABLIndicator,Facility_cms_in_{*}.REPortfolioType,
    Facility_cms_in_{*}.EnrollList,Collateral_cms_out_{*}.CollateralID,Collateral_cms_out_{*}.SegmentID,Collateral_cms_out_{*}.ActualLTV,Collateral_cms_out_{*}.SegmentIDUpdateDate,Collateral_cms_out_{*}.CollateralCountryCode,Collateral_cms_out_{*}.LGDProductPurposeCode,Collateral_cms_out_{*}.AllocationValue,Collateral_cms_out_{*}.UCCIndicator,Collateral_cms_out_{*}.LevelErrorList,Collateral_cms_in_{*}.GuaranteeID,Collateral_cms_in_{*}.PledgedType,
    Collateral_cms_out_{*}.CollateralTypeCd,Collateral_cms_in_{*}.CollateralSubTypeCd,Collateral_cms_in_{*}.CollateralProvinceStateCd,Collateral_cms_in_{*}.CollateralMarketAmountCAD,Collateral_cms_in_{*}.CollateralBookAmountCAD,Collateral_cms_out_{*}.CollateralAppraisedAmountCAD,Collateral_cms_in_{*}.PriorLiensAmountCAD,Collateral_cms_in_{*}.ChargeAmountCAD,Collateral_cms_in_{*}.EligibleCollateralAmountCAD,Collateral_cms_in_{*}.CollateralAssignmentTypeCd,Collateral_cms_out_{*}.LGDProductPurposeCode,Collateral_cms_out_{*}.ProvidedByCustomerId,Collateral_cms_out_{*}.UCCIndicator,Collateral_cms_out_{*}.ValuationMethod,
    Collateral_cms_out_{*}.ValuationDate,Collateral_cms_out_{*}.UnderWrittenValue,Collateral_cms_out_{*}.CROCOComplaintIndicator,Collateral_cms_out_{*}.ErrorList,Guarantee_cms_in_{*}.GuaranteeID,
    Guarantee_cms_in_{*}.GuaranteeType,Guarantee_cms_in_{*}.GuaranteeSubType,Guarantee_cms_in_{*}.GBRR,Guarantee_cms_in_{*}.GuaranteeDocumentId,Guarantee_cms_in_{*}.GuaranteeIndicators,Guarantee_cms_in_{*}.GuaranteeAssignmentType,Guarantee_cms_in_{*}.GuaranteeCAD,Guarantee_cms_in_{*}.GuarantorType,
    Guarantee_cms_in_{*}.GuarantorSICCode,Guarantee_cms_in_{*}.GuarantorSectorTypeCode,Guarantee_cms_in_{*}.GuarantorBSC,Guarantee_cms_in_{*}.GuarantorCountyCode,Guarantee_cms_in_{*}.GuarantorCTS
FROM 
    Facility_cms_out_{*}
    LEFT JOIN Facility_cms_in_{*} 
        ON Facility_cms_out_{*}.FacilityID = Facility_cms_in_{*}.FacilityID
       AND Facility_cms_out_{*}.cmsBorrowerCTS = Facility_cms_in_{*}.BorrowerCTS
    LEFT JOIN Collateral_cms_out_{*} 
        ON Facility_cms_out_{*}.FacilityID = Collateral_cms_out_{*}.FacilityID
    LEFT JOIN Collateral_cms_in_{*} 
        ON Collateral_cms_out_{*}.CollateralID = Collateral_cms_in_{*}.CollateralID
    LEFT JOIN Borrower_cms_in_{*} 
        ON Collateral_cms_out_{*}.cmsBorrowerCTS = Borrower_cms_in_{*}.cmsBorrowerCTS
    LEFT JOIN Pledge_cms_in_{*} 
        ON Collateral_cms_out_{*}.CollateralID = Pledge_cms_in_{*}.CollateralID
    LEFT JOIN Guarantee_cms_in_{*} 
        ON Pledge_cms_in_{*}.GuaranteeID = Guarantee_cms_in_{*}.GuaranteeID
"""

# CNB SQL
cnb_sql_query = """
SELECT 
    Facility_cnb_in_{*}.deposit, Facility_cnb_in_{*}.SharedLimitTXID, Facility_cnb_in_{*}.UCCIndicator,
    Facility_cnb_in_{*}.CREType, Facility_cnb_in_{*}.ABLIndicator,Facility_cnb_in_{*}.REPortfolioType,
    Facility_cnb_in_{*}.EnrollList,Collateral_cnb_out_{*}.CollateralID,Collateral_cnb_out_{*}.SegmentID,Collateral_cnb_out_{*}.ActualLTV,Collateral_cnb_out_{*}.SegmentIDUpdateDate,Collateral_cnb_out_{*}.CollateralCountryCode,Collateral_cnb_out_{*}.LGDProductPurposeCode,Collateral_cnb_out_{*}.AllocationValue,Collateral_cnb_out_{*}.UCCIndicator,Collateral_cnb_out_{*}.LevelErrorList,Collateral_cnb_in_{*}.GuaranteeID,Collateral_cnb_in_{*}.PledgedType,
    Collateral_cnb_out_{*}.CollateralTypeCd,Collateral_cnb_in_{*}.CollateralSubTypeCd,Collateral_cnb_in_{*}.CollateralProvinceStateCd,Collateral_cnb_in_{*}.CollateralMarketAmountCAD,Collateral_cnb_in_{*}.CollateralBookAmountCAD,Collateral_cnb_out_{*}.CollateralAppraisedAmountCAD,Collateral_cnb_in_{*}.PriorLiensAmountCAD,Collateral_cnb_in_{*}.ChargeAmountCAD,Collateral_cnb_in_{*}.EligibleCollateralAmountCAD,Collateral_cnb_in_{*}.CollateralAssignmentTypeCd,Collateral_cnb_out_{*}.LGDProductPurposeCode,Collateral_cnb_out_{*}.ProvidedByCustomerId,Collateral_cnb_out_{*}.UCCIndicator,Collateral_cnb_out_{*}.ValuationMethod,
    Collateral_cnb_out_{*}.ValuationDate,Collateral_cnb_out_{*}.UnderWrittenValue,Collateral_cnb_out_{*}.CROCOComplaintIndicator,Collateral_cnb_out_{*}.ErrorList,Guarantee_cnb_in_{*}.GuaranteeID,
    Guarantee_cnb_in_{*}.GuaranteeType,Guarantee_cnb_in_{*}.GuaranteeSubType,Guarantee_cnb_in_{*}.GBRR,Guarantee_cnb_in_{*}.GuaranteeDocumentId,Guarantee_cnb_in_{*}.GuaranteeIndicators,Guarantee_cnb_in_{*}.GuaranteeAssignmentType,Guarantee_cnb_in_{*}.GuaranteeCAD,Guarantee_cnb_in_{*}.GuarantorType,
    Guarantee_cnb_in_{*}.GuarantorSICCode,Guarantee_cnb_in_{*}.GuarantorSectorTypeCode,Guarantee_cnb_in_{*}.GuarantorBSC,Guarantee_cnb_in_{*}.GuarantorCountyCode,Guarantee_cnb_in_{*}.GuarantorCTS
FROM 
    Facility_cnb_out_{*}
    LEFT JOIN Facility_cnb_in_{*} 
        ON Facility_cnb_out_{*}.FacilityID = Facility_cnb_in_{*}.FacilityID
       AND Facility_cnb_out_{*}.CNBBorrowerCTS = Facility_cnb_in_{*}.CNBBorrowerCTS
    LEFT JOIN Collateral_cnb_out_{*} 
        ON Facility_cnb_out_{*}.FacilityID = Collateral_cnb_out_{*}.FacilityID
    LEFT JOIN Collateral_cnb_in_{*} 
        ON Collateral_cnb_out_{*}.CollateralID = Collateral_cnb_in_{*}.CollateralID
    LEFT JOIN Borrower_cnb_in_{*} 
        ON Collateral_cnb_out_{*}.CNBBorrowerCTS = Borrower_cnb_in_{*}.CNBBorrowerCTS
    LEFT JOIN Pledge_cnb_in_{*} 
        ON Collateral_cnb_out_{*}.CollateralID = Pledge_cnb_in_{*}.CollateralID
    LEFT JOIN Guarantee_cnb_in_{*} 
        ON Pledge_cnb_in_{*}.GuaranteeID = Guarantee_cnb_in_{*}.GuaranteeID
"""



###########################################
# --- New helper: clear and extract only .psv files into extracted folder ---
def _extract_psvs_to_extracted(tar_path: Path, module: str) -> List[Path]:
    """Extract inner archives (both in/out) and copy all .psv files into Extracted_psv_files/<module>.

    Returns list of extracted PSV paths (absolute).
    """
    out_dir = _module_to_extracted.get(module)
    if out_dir is None:
        raise ValueError(f'Unknown module {module}')
    # clear target folder
    if out_dir.exists():
        for p in out_dir.iterdir():
            try:
                if p.is_file():
                    p.unlink()
                else:
                    shutil.rmtree(p)
            except Exception:
                pass
    out_dir.mkdir(parents=True, exist_ok=True)

    if tar_path is None:
        return []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        # extract outer tar
        try:
            with tarfile.open(tar_path) as t:
                t.extractall(path=tmp)
        except Exception:
            # allow gz or tar depending on archive - rethrow to let caller see failure
            raise

        # Prepare a folder to collect contents of any inner archives
        extract_root = tmp
        inner_extract_root = tmp / 'inner_all'
        inner_extracted_any = False
        # Find inner archives (commonly ccms_in_*.tar and ccms_out_*.tar) and extract them.
        inner_candidates = []
        # prefer helper if available for specific keywords, but also scan for common archive extensions
        if _find_inner_out_tar is not None:
            # try to collect any likely inner archives using helper for both in/out keywords
            for kw in [module + '_in', module + '_out', module + '_out', 'esn_out', 'cms_out', 'ccms_out', 'cnb_out', 'lgd_commercial', 'in', 'out']:
                try:
                    it = _find_inner_out_tar(tmp, kw)
                    if it:
                        inner_candidates.append(Path(it))
                except Exception:
                    pass
        # generic scan for common archive types inside the outer tar extraction
        for pat in ('*.tar', '*.tar.gz', '*.tgz', '*.gz', '*.zip'):
            for p in tmp.rglob(pat):
                try:
                    if p.is_file():
                        inner_candidates.append(p)
                except Exception:
                    continue
        # dedupe while preserving order
        seen = set()
        inner_candidates_filtered = []
        for p in inner_candidates:
            if str(p) not in seen:
                seen.add(str(p))
                inner_candidates_filtered.append(p)

        if inner_candidates_filtered:
            inner_extract_root.mkdir(exist_ok=True)
            for inner in inner_candidates_filtered:
                try:
                    # handle tar-like archives
                    if inner.suffix in ('.tar',) or inner.name.lower().endswith(('.tar.gz', '.tgz', '.tar')):
                        try:
                            with tarfile.open(inner) as it:
                                it.extractall(path=inner_extract_root)
                                inner_extracted_any = True
                        except Exception:
                            # some .gz files are not tar archives; skip if cannot open as tar
                            continue
                    elif inner.suffix == '.gz' and not inner.name.lower().endswith(('.tar.gz', '.tgz')):
                        # skip lone gz files (likely single-file compression) unless they are tars handled above
                        try:
                            with tarfile.open(inner) as it:
                                it.extractall(path=inner_extract_root)
                                inner_extracted_any = True
                        except Exception:
                            continue
                    elif inner.suffix == '.zip':
                        try:
                            with zipfile.ZipFile(inner, 'r') as z:
                                z.extractall(path=inner_extract_root)
                                inner_extracted_any = True
                        except Exception:
                            continue
                except Exception:
                    continue
            if inner_extracted_any:
                extract_root = inner_extract_root

        # Now copy all .psv files found under extract_root into out_dir
        found = []
        for p in extract_root.rglob('*.psv'):
            try:
                dest = out_dir / p.name
                shutil.copy2(p, dest)
                found.append(dest)
            except Exception:
                continue
        return found



# --- New helper: export a dataframe to formatted Excel ---
# python
def _export_df_to_excel(df: pd.DataFrame, module: str, report_dir: Path) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_xlsx = report_dir / f'{module.lower()}_consolidated_report_{ts}.xlsx'
    sheet_name = f"{module.upper()}_Consolidated"

    # If df is None or has no columns, create a safe placeholder so to_excel doesn't raise.
    placeholder = False
    if df is None or (hasattr(df, "empty") and df.empty) or (hasattr(df, "columns") and len(df.columns) == 0):
        df_to_write = pd.DataFrame({"No Data": [""]})
        placeholder = True
    else:
        df_to_write = df.copy()

    # write dataframe to module-specific sheet name using xlsxwriter and apply formatting
    with pd.ExcelWriter(out_xlsx, engine='xlsxwriter') as writer:
        # Always write the safe dataframe (original or placeholder)
        df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)
        max_row = (df_to_write.shape[0] if hasattr(df_to_write, 'shape') else 0) + 1
        max_col = (df_to_write.shape[1] if hasattr(df_to_write, 'shape') else 0)

        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        # define formats: header (bold) and normal cell (centered) with thin border
        header_fmt = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        cell_fmt = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1})

        # If df_to_write has columns, apply formatting
        if max_col > 0:
            # header row is row 0 in ExcelWriter output
            for col_idx, col_name in enumerate(df_to_write.columns):
                try:
                    worksheet.write(0, col_idx, str(col_name), header_fmt)
                except Exception:
                    pass

            # data rows
            for r in range(1, max_row):
                for c in range(max_col):
                    try:
                        val = df_to_write.iat[r-1, c] if (r-1) < getattr(df_to_write, "shape", (0,0))[0] else ''
                    except Exception:
                        val = ''
                    if val is None:
                        val = ''
                    try:
                        worksheet.write(r, c, str(val), cell_fmt)
                    except Exception:
                        try:
                            worksheet.write(r, c, '', cell_fmt)
                        except Exception:
                            pass

    return out_xlsx



# --- New helper: zip report files ---
def _zip_report_files(paths: List[Path], out_zip: Path) -> Path:
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_zip, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        for p in paths:
            z.write(p, arcname=p.name)
    return out_zip



# --- New function: programmatic generator using embedded SQLs ---
def generate_consolidated_from_constants(ccms_choice: str = 'Without CRE'):
    """Extract PSV files from Uploaded_files, ensure required PSVs/columns exist,
    execute embedded SQLs and export consolidated xlsx files for ccms/cms/cnb.
    Returns list of generated xlsx Paths.
    """
    generated = []
    # choose CCMS SQL based on choice
    ccms_sql = ccms_sql_query_with_cre if (ccms_choice and ccms_choice.strip().lower() == 'with cre') else ccms_sql_query_without_cre
    modules = [
        ('ccms', UPLOADED_CC, ccms_sql, REPORTS_CC, EXTRACTED_CC),
        ('cms', UPLOADED_CMS, cms_sql_query, REPORTS_CMS, EXTRACTED_CMS),
        ('cnb', UPLOADED_CNB, cnb_sql_query, REPORTS_CNB, EXTRACTED_CNB),
    ]

    for module, upload_dir, sql_text, report_dir, extract_dir in modules:
        # find a tar in upload_dir (pick newest)
        tar_files = list(upload_dir.glob('*')) if upload_dir.exists() else []
        tar_files = [p for p in tar_files if p.is_file() and (p.suffix == '.tar' or p.name.endswith('.tar.gz') or p.suffix == '.gz')]
        if not tar_files:
            print(f'[WARN] No uploaded tar found for {module} in {upload_dir}; skipping')
            continue
        tar_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        tar_path = tar_files[0]
        print(f'[INFO] Using {tar_path} for module {module}')
        # extract PSVs into extracted folder
        extracted = _extract_psvs_to_extracted(tar_path, module)
        print(f'[INFO] Extracted {len(extracted)} PSV files into {extract_dir}')
        # parse SQL
        parsed = _parse_simple_sql(sql_text)
        df_map, missing_files, missing_cols = _load_psvs_for_sql(parsed, module)
        # If missing, create dummy PSV files
        if missing_files or missing_cols:
            print(f'[ERROR] Missing inputs for {module}: files={missing_files} cols={missing_cols}; skipping generation for this module.')
            continue
        # execute parsed SQL
        try:
            df_final = _execute_parsed_sql(parsed, df_map)
        except Exception as e:
            print(f'[ERROR] Failed to execute SQL for {module}: {e}')
            continue
        # export
        try:
            out = _export_df_to_excel(df_final, module, report_dir)
            generated.append(out)
            print(f'[INFO] Generated consolidated report for {module}: {out}')
        except Exception as e:
            print(f'[ERROR] Failed to write report for {module}: {e}')

    return generated
#####################################################################

# Do not delete any function here, Please modify this according to below procedure

# 1) Embedded SqL are given for CCMS with and without CRE, CMS and CNB in the above code. In all SQL query '*" is there, it means that in place of '*' actual timestamp will be there in the PSV file name which is present in extracted folder for respective module after extraction of psv files from uploaded/selected tar file. So while executing SQL query on duckdb library, you have to replace '*' with actual timestamp which is present in extracted PSV file name for respective module. For example if Facility_ccms_in_20250403_202505232005.psv is present in extracted folder for CCMS then you have to replace Facility_ccms_in_{*}.psv with Facility_ccms_in_20250403_202505232005.psv in embedded SQL query before executing on duckdb.
# 2) required_files_for_ccms, required_files_for_cms and required_files_for_cnb are the lists which contains the required PSV file patterns for each module.
# 3) Post click on 'Upload file and Verify (CCMS)' button and then cCms_report_generation flag will become True and all files are deleted which are present in 'rbc_uat_automation/data/consolidated_generator/Extracted_psv_files/ccms' and then all psv files only are extracted from uploaded / selected TAR files and stored into 'rbc_uat_automation/data/consolidated_generator/Extracted_psv_files/ccms'.
# 4) The list of required_files_for_ccms must be present in 'rbc_uat_automation/data/consolidated_generator/Extracted_psv_files/ccms' folder after extraction of psv files, if any file is missing then it will show error message to user on UI and generation process will be stopped until all required files are present in extracted folder for CCMS only.
# 5) If all required files are present in extracted folder in CCMS then all psv files data (All data should be in string) are converted into table and stored into duckdb library as as table under CCMS schema and then respective embedded SQL query will be executed on those tables and final consolidated report in xlsx format will be generated with help of xlsxwriter library (Do not use openpyxl) and stored into 'rbc_uat_automation/reports/consolidated_generator/ccms' folder with name 'ccms_consolidated_report_{timestamp}.xlsx'.
# 6) For CCMS, User will select "With CRE" or "Without CRE" from dropdown and based on that respective embedded SQL query will be executed and consolidated report will be generated.
# 7) All data should have in table format with proper border for each cell those have data in cell.
# 8) For CMS and CNB, there is noe any 'With CRE' or 'Without CRE' option, so only one embedded SQL query will be executed for each module and consolidated report will be generated in xlsx format with proper formatting as mentioned in point 7.
# 9) Follow the same for CMS and CNB module as well but with their respective required files list, embedded SQL query and report storage folder.


######################################################


def _reset_consolidated_state():
    """Reset Streamlit session state items related to consolidated generation so the user can start fresh."""
    # Do NOT set file_uploader-backed keys here (e.g. 'ccms_upload') —
    # Streamlit raises an error if you set them via session_state.
    keys_defaults = {
        'consol_ccms_ready': False,
        'consol_cms_ready': False,
        'consol_cnb_ready': False,
        'ccms_report_generation': False,
        'cms_report_generation': False,
        'cnb_report_generation': False,
        'ccms_verified': '',
        'cms_verified': '',
        'cnb_verified': '',
        'ccms_select': '',
        'cms_select': '',
        'cnb_select': '',
        'ccms_sql': ccms_sql_query_with_cre if st.session_state.get('ccms_query_type','Without CRE').strip().lower() == 'with cre' else ccms_sql_query_without_cre,
        'cms_sql': cms_sql_query,
        'cnb_sql': cnb_sql_query,
        'ccms_query_type_prev': None,
        'ccms_query_type': 'Without CRE',
    }
    for k, v in keys_defaults.items():
        st.session_state[k] = v
    # reset matched maps
    st.session_state.pop('ccms_matched_map', None)
    st.session_state.pop('cms_matched_map', None)
    st.session_state.pop('cnb_matched_map', None)


def view():
    # Ensure required folders(uploads, reports) exist on Root
    list_of_dirs = [UPLOADED_CC, UPLOADED_CMS, UPLOADED_CNB, EXTRACTED,EXTRACTED_CC, EXTRACTED_CMS, EXTRACTED_CNB, REPORTS_CC, REPORTS_CMS, REPORTS_CNB]
    _ensure_dirs(list_of_dirs)

    st.markdown("# LGD UAT Automation Solution", text_alignment="center")
    st.title('📊 Consolidated Report Generator', text_alignment="center")
    st.caption('Upload or Select CCMS/CMS/CNB TAR files to generate Consolidated reports', text_alignment="center")

    # Mark that we are on the consolidated page so other parts of app can detect it
    st.session_state['home_consolidated'] = True

    # Refresh button for consolidated page (top-right of CCMS section)
    if st.button('Refresh', key='refresh_top'):
        _reset_consolidated_state()
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

    if 'ccms_verified' not in st.session_state:
        st.session_state['ccms_verified'] = ''
    if 'cms_verified' not in st.session_state:
        st.session_state['cms_verified'] = ''
    if 'cnb_verified' not in st.session_state:
        st.session_state['cnb_verified'] = ''

    # consolidated ready flags
    if 'consol_ccms_ready' not in st.session_state:
        st.session_state['consol_ccms_ready'] = False
    if 'consol_cms_ready' not in st.session_state:
        st.session_state['consol_cms_ready'] = False
    if 'consol_cnb_ready' not in st.session_state:
        st.session_state['consol_cnb_ready'] = False

    # SQL input defaults
    if 'ccms_sql' not in st.session_state:
        st.session_state['ccms_sql'] = ''
    if 'cms_sql' not in st.session_state:
        st.session_state['cms_sql'] = ''
    if 'cnb_sql' not in st.session_state:
        st.session_state['cnb_sql'] = ''

    # CCMS section
    st.subheader('CCMS Section:---')

    ccms_select = st.selectbox('Select CCMS TAR File', options=[''] + _recent_files(UPLOADED_CC), key='ccms_select')
    ccms_upload = st.file_uploader('Or Upload CCMS TAR File', type=['tar', 'gz'],key='ccms_upload')
    ccms_path = None
    if ccms_upload is not None:
        if not _validate_ccms_filename(ccms_upload.name):
            st.error("Please upload 'lgd_ccms_in_out_{timestamp}.tar.gz' or 'ccms_out_{timestamp}.tar.gz'")
        else:
            ccms_path = _save_uploaded(ccms_upload, UPLOADED_CC)
            st.success(f'Uploaded to {ccms_path}')
    elif ccms_select:
        ccms_path = UPLOADED_CC / ccms_select


    # Keep verification only while selected/uploaded files for CCMS remain unchanged
    current_ccms = str(ccms_path) if ccms_path is not None else ''

    # New: select box for predefined query type (With CRE / Without CRE)
    ccms_query_type = st.selectbox('Predefined query type (CCMS)', options=['Without CRE', 'With CRE'], index=0)
    # maintain previous choice to detect change
    prev_choice = st.session_state.get('ccms_query_type_prev')
    st.session_state['ccms_query_type'] = ccms_query_type
    # Choose default SQL based on selection
    chosen_ccms_sql = ccms_sql_query_with_cre if ccms_query_type.strip().lower() == 'with cre' else ccms_sql_query_without_cre
    # If SQL area not set yet or user changed the CRE selection, initialize/update it
    if st.session_state.get('ccms_sql','') == '' or prev_choice != ccms_query_type:
        st.session_state['ccms_sql'] = chosen_ccms_sql
    # store prev
    st.session_state['ccms_query_type_prev'] = ccms_query_type

    # Show an editable SQL text area for CCMS (user can edit before generating)
    st.markdown('**CCMS SQL (Read-Only)**')
    # Display CCMS SQL as Read-Only (user cannot edit in the UI)
    st.text_area('CCMS SQL (Read-Only)', value=st.session_state.get('ccms_sql',''), key='ccms_sql', height=220, disabled=True)

    # If files changed since last verification, clear the verified flag
    if st.session_state.get('ccms_verified', '') != current_ccms:
        # Only clear the generation flag if the stored verification does not match current selection
        st.session_state['ccms_report_generation'] = False

    if st.session_state.get('ccms_report_generation') and st.session_state.get('ccms_verified') == current_ccms:
        st.success('CCMS files verified')
    else:
        if ccms_path :
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
            with col5:
                if st.button('Upload File and Verify (CCMS)', key='verify_ccms'):
                    with st.spinner(f'Extracting PSV files from {ccms_path}'):  # type: ignore
                        _extract_psvs_to_extracted(ccms_path, 'ccms')
                    # parse CCMS SQL and compute matched PSV filenames to show the user
                    try:
                        parsed = _parse_simple_sql(st.session_state.get('ccms_sql',''))
                        matched_map = _matched_psv_paths(parsed, 'ccms')
                        # persist a token->filename map (filename or None) so we can render left-aligned below
                        map_dict = {t: (p.name if p is not None else None) for t, p in matched_map.items()}
                        st.session_state['ccms_matched_map'] = map_dict
                        # compute quick lists for warnings (kept for backward compatibility)
                        matched_list = [fn for fn in map_dict.values() if fn]
                        missing_tokens = [t for t, fn in map_dict.items() if fn is None]
                        # don't render here (centered); render left-aligned after this verify block
                    except Exception:
                        # ignore parsing/display failure, continue with existing behavior
                        pass
                    st.session_state['ccms_report_generation'] = True
                    st.session_state['ccms_verified'] = current_ccms
                    # mark consolidated ready for CCMS
                    st.session_state['consol_ccms_ready'] = True
                    st.success('CCMS files verified')
        else:
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
            with col5:
                st.button('Upload File and Verify (CCMS)', disabled=True, key='verify_ccms_disabled')

    # Left-aligned display of the matched PSV files (token -> filename)
    # Rendered here (outside the centered columns) so it appears at the left margin.
    if st.session_state.get('ccms_matched_map'):
        try:
            token_rows_display = [{'token': t, 'matched_filename': (fn or 'NOT FOUND')} for t, fn in st.session_state.get('ccms_matched_map', {}).items()]
            with st.expander('Matched PSV files (token -> filename)', expanded=False):
                # Render as a simple markdown bullet list so items align at the left margin
                for row in token_rows_display:
                    fn = row.get('matched_filename') or 'NOT FOUND'
                    st.markdown(f'- {fn}')
        except Exception:
            for t, fn in st.session_state.get('ccms_matched_map', {}).items():
                st.markdown(f'- {fn or "NOT FOUND"}')

    st.markdown('---')


    ###############################################

    # CMS section
    st.subheader('CMS Section:---')

    cms_select = st.selectbox('Select CMS TAR File', options=[''] + _recent_files(UPLOADED_CMS), key='cms_select')
    cms_upload = st.file_uploader('Or Upload CMS TAR File', type=['tar', 'gz'], key='cms_upload')
    cms_path = None
    if cms_upload is not None:
        if not _validate_cms_filename(cms_upload.name):
            st.error("Please upload 'lgd_commercial_in_out_{timestamp}.tar.gz' or 'esn_out_{timestamp}.tar.gz' or 'cms_out_{timestamp}.tar.gz'")
        else:
            cms_path = _save_uploaded(cms_upload, UPLOADED_CMS)
            st.success(f'Uploaded to {cms_path}')
    elif cms_select:
        cms_path = UPLOADED_CMS / cms_select

    # Keep verification only while selected/uploaded files for CMS remain unchanged
    current_cms = str(cms_path) if cms_path is not None else ''

    # After CMS verify UI, ensure CMS SQL area exists and is prefilled
    # Initialize CMS SQL if empty
    if st.session_state.get('cms_sql','') == '':
        st.session_state['cms_sql'] = cms_sql_query
    st.markdown('**CMS SQL (Read-Only)**')
    # Display CMS SQL as Read-Only (user cannot edit in the UI)
    st.text_area('CMS SQL (Read-Only)', value=st.session_state.get('cms_sql',''), key='cms_sql', height=220, disabled=True)

    # If files changed since last verification, clear the verified flag
    if st.session_state.get('cms_verified', '') != current_cms:
        # Only clear the generation flag if the stored verification does not match current selection
        st.session_state['cms_report_generation'] = False

    if st.session_state.get('cms_report_generation') and st.session_state.get('cms_verified') == current_cms:
        st.success('CMS files verified')
    else:
        if cms_path:
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
            with col5:
                if st.button('Upload File and Verify (CMS)', key='verify_cms'):
                    with st.spinner(f'Extracting PSV files from {cms_path}'):  # type: ignore
                        _extract_psvs_to_extracted(cms_path, 'cms')
                    # parse CMS SQL and persist matched PSV filenames to session state
                    try:
                        parsed = _parse_simple_sql(st.session_state.get('cms_sql',''))
                        matched_map = _matched_psv_paths(parsed, 'cms')
                        map_dict = {t: (p.name if p is not None else None) for t, p in matched_map.items()}
                        st.session_state['cms_matched_map'] = map_dict
                        matched_list = [fn for fn in map_dict.values() if fn]
                        missing_tokens = [t for t, fn in map_dict.items() if fn is None]
                        # UI rendering moved below (left-aligned)
                    except Exception:
                        pass
                    st.session_state['cms_report_generation'] = True
                    st.session_state['cms_verified'] = current_cms
                    # mark consolidated ready for CMS
                    st.session_state['consol_cms_ready'] = True
                    st.success('CMS files verified')
        else:
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
            with col5:
                st.button('Upload File and Verify (CMS)', disabled=True, key='verify_cms_disabled')

    # Left-aligned display of the matched PSV files for CMS
    if st.session_state.get('cms_matched_map'):
        try:
            token_rows_display = [{'token': t, 'matched_filename': (fn or 'NOT FOUND')} for t, fn in st.session_state.get('cms_matched_map', {}).items()]
            with st.expander('Matched PSV files (token -> filename)', expanded=False):
                for row in token_rows_display:
                    fn = row.get('matched_filename') or 'NOT FOUND'
                    st.markdown(f'- {fn}')
        except Exception:
            for t, fn in st.session_state.get('cms_matched_map', {}).items():
                st.markdown(f'- {fn or "NOT FOUND"}')

    st.markdown('---')

    ###############################################

    # CNB section
    st.subheader('CNB Section:---')

    cnb_select = st.selectbox('Select CNB TAR File', options=[''] + _recent_files(UPLOADED_CNB), key='cnb_select')
    cnb_upload = st.file_uploader('Or Upload CNB TAR File', type=['tar', 'gz'], key='cnb_upload')
    cnb_path = None
    if cnb_upload is not None:
        if not _validate_cnb_filename(cnb_upload.name):
            st.error("Please upload 'cnb_in_out_{timestamp}.tar.gz' or 'cnb_out_{timestamp}.tar.gz'")
        else:
            cnb_path = _save_uploaded(cnb_upload, UPLOADED_CNB)
            st.success(f'Uploaded to {cnb_path}')
    elif cnb_select:
        cnb_path = UPLOADED_CNB / cnb_select

    # Keep verification only while selected/uploaded files for CNB remain unchanged
    current_cnb = str(cnb_path) if cnb_path is not None else ''

    # After CNB verify UI, ensure CNB SQL area exists and is prefilled
    if st.session_state.get('cnb_sql','') == '':
        st.session_state['cnb_sql'] = cnb_sql_query
    st.markdown('**CNB SQL (Read-Only)**')
    # Display CNB SQL as Read-Only (user cannot edit in the UI)
    st.text_area('CNB SQL (Read-Only)', value=st.session_state.get('cnb_sql',''), key='cnb_sql', height=220, disabled=True)

    # If files changed since last verification, clear the verified flag
    if st.session_state.get('cnb_verified', '') != current_cnb:
        # Only clear the generation flag if the stored verification does not match current selection
        st.session_state['cnb_report_generation'] = False

    if st.session_state.get('cnb_report_generation') and st.session_state.get('cnb_verified') == current_cnb:
        st.success('CNB files verified')
    else:
        if cnb_path:
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
            with col5:
                if st.button('Upload File and Verify (CNB)', key='verify_cnb'):
                    with st.spinner(f'Extracting PSV files from {cnb_path}'):  # type: ignore
                        _extract_psvs_to_extracted(cnb_path, 'cnb')
                    # parse CNB SQL and persist matched PSV filenames to session state
                    try:
                        parsed = _parse_simple_sql(st.session_state.get('cnb_sql',''))
                        matched_map = _matched_psv_paths(parsed, 'cnb')
                        map_dict = {t: (p.name if p is not None else None) for t, p in matched_map.items()}
                        st.session_state['cnb_matched_map'] = map_dict
                        matched_list = [fn for fn in map_dict.values() if fn]
                        missing_tokens = [t for t, fn in map_dict.items() if fn is None]
                        # UI rendering moved below (left-aligned)
                    except Exception:
                        pass
                    st.session_state['cnb_report_generation'] = True
                    st.session_state['cnb_verified'] = current_cnb
                    # mark consolidated ready for CNB
                    st.session_state['consol_cnb_ready'] = True
                    st.success('CNB files verified')
        else:
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
            with col5:
                st.button('Upload File and Verify (CNB)', disabled=True, key='verify_cnb_disabled')

    # Left-aligned display of the matched PSV files for CNB
    if st.session_state.get('cnb_matched_map'):
        try:
            token_rows_display = [{'token': t, 'matched_filename': (fn or 'NOT FOUND')} for t, fn in st.session_state.get('cnb_matched_map', {}).items()]
            with st.expander('Matched PSV files (token -> filename)', expanded=False):
                for row in token_rows_display:
                    fn = row.get('matched_filename') or 'NOT FOUND'
                    st.markdown(f'- {fn}')
        except Exception:
            for t, fn in st.session_state.get('cnb_matched_map', {}).items():
                st.markdown(f'- {fn or "NOT FOUND"}')

    st.markdown('---')

    # Enable generate when any of the modules is flagged ready
    if st.session_state.get('consol_ccms_ready') or st.session_state.get('consol_cms_ready') or st.session_state.get('consol_cnb_ready'):
        # Create stable placeholders for per-module progress UI so widgets get stable IDs and are not recreated
        module_placeholders = {
            'ccms': st.empty(),
            'cms': st.empty(),
            'cnb': st.empty(),
        }
        # placeholders reserved; will populate them when generation starts

        if st.button('Generate Consolidated Report', key='generate_consolidated'):
            reports_generated = []
            errors_found = []
            # Setup overall progress UI similar to regression_generator
            progress_bar = st.progress(0)
            status_box = st.empty()

            # determine which modules to run
            modules_to_run = []
            if st.session_state.get('consol_ccms_ready'):
                modules_to_run.append('ccms')
            if st.session_state.get('consol_cms_ready'):
                modules_to_run.append('cms')
            if st.session_state.get('consol_cnb_ready'):
                modules_to_run.append('cnb')

            # create per-module small progress widgets using the placeholders (stable positions)
            progress_widgets = {}
            for mod in modules_to_run:
                container = module_placeholders[mod]
                container.markdown(f'**{mod.upper()} Report Progress:**')
                pb = container.progress(0)
                stbox = container.empty()
                progress_widgets[mod] = (pb, stbox)

            def make_cb(mod_name):
                def cb(pct, msg):
                    try:
                        # update overall progress bar only
                        try:
                            progress_bar.progress(min(max(int(pct), 0), 100))
                        except Exception:
                            pass
                        # per-module status box only (avoid duplicating messages in overall status_box)
                        pair = progress_widgets.get(mod_name)
                        if pair:
                            pb, stbox = pair
                            try:
                                pb.progress(min(max(int(pct), 0), 100))
                            except Exception:
                                pass
                            try:
                                stbox.info(msg)
                            except Exception:
                                pass
                    except Exception:
                        pass
                return cb

            # run modules sequentially with callbacks to update UI
            # CCMS
            if st.session_state.get('consol_ccms_ready'):
                cb = make_cb('ccms')
                try:
                    cb(0, 'Starting CCMS consolidated generation')
                    sql_text = st.session_state.get('ccms_sql','')
                    if not sql_text.strip():
                        ccms_choice = st.session_state.get('ccms_query_type','Without CRE')
                        sql_text = ccms_sql_query_with_cre if ccms_choice.strip().lower() == 'with cre' else ccms_sql_query_without_cre
                        st.session_state['ccms_sql'] = sql_text
                    cb(20, 'Parsing CCMS SQL')
                    if not sql_text.strip():
                        errors_found.append('CCMS SQL is empty; skipping CCMS')
                    else:
                        parsed = _parse_simple_sql(sql_text)
                        cb(35, 'Loading CCMS PSV files')
                        df_map, missing_files, missing_cols = _load_psvs_for_sql(parsed, 'ccms')
                        if missing_files or missing_cols:
                            cb(60, 'Missing PSV files or columns for CCMS; skipping CCMS generation')
                            errors_found.append(f'CCMS missing inputs: files={missing_files} cols={missing_cols}')
                            raise StopIteration
                        cb(75, 'Executing CCMS SQL joins')
                        df_final = _execute_parsed_sql(parsed, df_map)
                        cb(90, 'Exporting CCMS consolidated Excel')
                        out = _export_df_to_excel(df_final, 'ccms', REPORTS_CC)
                        reports_generated.append(out)
                        cb(100, 'CCMS generation completed')
                except Exception as e:
                    errors_found.append(f'CCMS error: {e}')

            # CMS
            if st.session_state.get('consol_cms_ready'):
                cb = make_cb('cms')
                try:
                    cb(0, 'Starting CMS consolidated generation')
                    sql_text = st.session_state.get('cms_sql','') or cms_sql_query
                    cb(20, 'Parsing CMS SQL')
                    parsed = _parse_simple_sql(sql_text)
                    cb(40, 'Loading CMS PSV files')
                    df_map, missing_files, missing_cols = _load_psvs_for_sql(parsed, 'cms')
                    if missing_files or missing_cols:
                        cb(60, 'Missing PSV files or columns for CMS; skipping CMS generation')
                        errors_found.append(f'CMS missing inputs: files={missing_files} cols={missing_cols}')
                        raise StopIteration
                    cb(80, 'Executing CMS SQL joins')
                    df_final = _execute_parsed_sql(parsed, df_map)
                    cb(90, 'Exporting CMS consolidated Excel')
                    out = _export_df_to_excel(df_final, 'cms', REPORTS_CMS)
                    reports_generated.append(out)
                    cb(100, 'CMS generation completed')
                except Exception as e:
                    errors_found.append(f'CMS error: {e}')

            # CNB
            if st.session_state.get('consol_cnb_ready'):
                cb = make_cb('cnb')
                try:
                    cb(0, 'Starting CNB consolidated generation')
                    sql_text = st.session_state.get('cnb_sql','') or cnb_sql_query
                    cb(20, 'Parsing CNB SQL')
                    parsed = _parse_simple_sql(sql_text)
                    cb(40, 'Loading CNB PSV files')
                    df_map, missing_files, missing_cols = _load_psvs_for_sql(parsed, 'cnb')
                    if missing_files or missing_cols:
                        cb(60, 'Missing PSV files or columns for CNB; skipping CNB generation')
                        errors_found.append(f'CNB missing inputs: files={missing_files} cols={missing_cols}')
                        raise StopIteration
                    cb(80, 'Executing CNB SQL joins')
                    df_final = _execute_parsed_sql(parsed, df_map)
                    cb(90, 'Exporting CNB consolidated Excel')
                    out = _export_df_to_excel(df_final, 'cnb', REPORTS_CNB)
                    reports_generated.append(out)
                    cb(100, 'CNB generation completed')
                except Exception as e:
                    errors_found.append(f'CNB error: {e}')

            if errors_found:
                for e in errors_found:
                    st.warning(e)
            if reports_generated:
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                zip_path = REPORTS_ROOT / f'consolidated_report_{ts}.zip'
                _zip_report_files(reports_generated, zip_path)
                with open(zip_path, 'rb') as f:
                    data = f.read()
                col_dl, col_ref = st.columns([3,1])
                with col_dl:
                    st.download_button('Download Consolidated Reports ZIP', data, file_name=zip_path.name, key='download_consolidated_zip')

def _matched_psv_paths(parsed: Dict, module: str) -> Dict[str, Optional[Path]]:
    """Return a mapping from table token (as in SQL) to the matched Path in the extracted folder or None."""
    mapping: Dict[str, Optional[Path]] = {}
    if parsed is None:
        return mapping
    tokens = []
    if parsed.get('from'):
        tokens.append(parsed['from'])
    for j in parsed.get('joins', []):
        rt = j.get('right_table')
        if rt:
            tokens.append(rt)
    # unique preserve order
    seen = set()
    tokens = [t for t in tokens if t and not (t in seen or seen.add(t))]
    for t in tokens:
        p = _find_psv_for_token(t, module)
        mapping[t] = p
    return mapping


def _parse_simple_sql(sql_text: str) -> Dict:
    """Very small SQL parser to extract FROM table token and JOIN table tokens.

    Expects tokens in the SQL to appear like: Facility_ccms_out_{*}
    Returns dict with keys: 'raw_sql', 'from', 'joins' where join entries are dicts with 'right_table'.
    """
    if not sql_text:
        return {'raw_sql': '', 'from': None, 'joins': []}
    txt = sql_text
    # normalize spacing
    low = txt
    # find first FROM token
    import re
    parsed = {'raw_sql': txt, 'from': None, 'joins': []}
    # match token pattern like Name_{*}
    token_re = re.compile(r"([A-Za-z0-9_]+_\{\*\})")
    # FROM clause
    m = re.search(r'FROM\s+([A-Za-z0-9_]+_\{\*\})', txt, re.IGNORECASE)
    if m:
        parsed['from'] = m.group(1)
    # JOIN occurrences (right table)
    for jm in re.finditer(r'JOIN\s+([A-Za-z0-9_]+_\{\*\})', txt, re.IGNORECASE):
        parsed['joins'].append({'right_table': jm.group(1)})
    return parsed


def _find_psv_for_token(token: str, module: str) -> Optional[Path]:
    """Find a PSV file in the module's extracted folder that matches the token pattern.

    Token may be like 'Facility_ccms_out_{*}' or 'Facility_ccms_out_{*}.psv'. We support both.
    Returns Path or None.
    """
    if token is None:
        return None
    # normalize pattern
    pattern = token
    if pattern.endswith('.psv'):
        pattern = pattern
    else:
        pattern = pattern + '.psv'
    # replace placeholder with glob
    pattern_glob = pattern.replace('{*}', '*')
    extract_dir = _module_to_extracted.get(module)
    if extract_dir is None or not extract_dir.exists():
        return None
    # try direct glob
    found = None
    for p in extract_dir.iterdir():
        if p.is_file() and fnmatch.fnmatch(p.name, pattern_glob):
            found = p
            break
    # also search recursively
    if found is None:
        for p in extract_dir.rglob('*'):
            try:
                if p.is_file() and fnmatch.fnmatch(p.name, pattern_glob):
                    found = p
                    break
            except Exception:
                continue
    return found


def _safe_read_psv(p: Path) -> pd.DataFrame:
    """***** SAFE PSV READER *****
    Try several strategies to read a .psv file and ensure we never return a DataFrame with zero columns.
    Returns a DataFrame (all columns as strings). If file appears malformed, returns a placeholder DF with one _diag_col.
    Marked with ***** so user can find changes.
    """
    try:
        # Prefer project's specialized reader if available
        if 'read_psv_preserve_shape' in globals() and callable(read_psv_preserve_shape):
            df = read_psv_preserve_shape(p)
            if isinstance(df, pd.DataFrame) and df.shape[1] > 0:
                return df.fillna('').astype(str)
    except Exception as e:
        print('***** _safe_read_psv: read_psv_preserve_shape failed:', e)
    # Try standard read with expected pipe separator
    try:
        df = pd.read_csv(p, sep='|', dtype=str, engine='python').fillna('').astype(str)
        if isinstance(df, pd.DataFrame) and df.shape[1] > 0:
            return df
    except Exception as e:
        print('***** _safe_read_psv: pd.read_csv with sep="|" failed:', e)
    # Try with header=None (sometimes files have no header row or parser mis-detected)
    try:
        df = pd.read_csv(p, sep='|', dtype=str, engine='python', header=None).fillna('').astype(str)
        # if we got at least one column, assign generic column names
        if isinstance(df, pd.DataFrame) and df.shape[1] > 0:
            df.columns = [f'col_{i}' for i in range(df.shape[1])]
            return df
    except Exception as e:
        print('***** _safe_read_psv: pd.read_csv header=None failed:', e)
    # Last resort: attempt to manually split first non-empty line by common delimiters
    try:
        with p.open('r', encoding='utf-8', errors='replace') as fh:
            lines = [ln.rstrip('\n') for ln in fh.readlines() if ln.strip()]
        if lines:
            # detect delimiter by checking common ones
            sample = lines[0]
            for delim in ['|', '\t', ',']:
                parts = sample.split(delim)
                if len(parts) > 1:
                    # build DataFrame by splitting each line
                    rows = [ln.split(delim) for ln in lines]
                    maxcols = max(len(r) for r in rows)
                    norm_rows = [r + [''] * (maxcols - len(r)) for r in rows]
                    df = pd.DataFrame(norm_rows)
                    df.columns = [f'col_{i}' for i in range(df.shape[1])]
                    return df.fillna('').astype(str)
    except Exception as e:
        print('***** _safe_read_psv: manual split attempt failed:', e)
    # If everything failed, return a placeholder single-column DataFrame so callers can detect malformed input
    print(f"***** _safe_read_psv: falling back to placeholder for {p}")
    return pd.DataFrame({'_diag_col': ['']})


def _load_psvs_for_sql(parsed: Dict, module: str) -> Tuple[Dict[str, pd.DataFrame], List[str], List[str]]:
    """Load required PSV files for a parsed SQL and return df_map, missing_files, missing_cols.

    df_map keys are the concrete table names (file stem) that should be used in SQL.
    Also populates parsed['token_to_table'] mapping tokens->actual table names.

    ***** CHANGES: Use _safe_read_psv and record files with zero columns in missing_cols. *****
    """
    df_map: Dict[str, pd.DataFrame] = {}
    missing_files: List[str] = []
    missing_cols: List[str] = []

    # verify required files for module exist
    req_list = []
    if module == 'ccms':
        req_list = required_files_for_ccms
    elif module == 'cms':
        req_list = required_files_for_cms
    elif module == 'cnb':
        req_list = required_files_for_cnb

    extract_dir = _module_to_extracted.get(module)
    if extract_dir is None:
        missing_files = req_list.copy()
        return df_map, missing_files, missing_cols

    # check required file patterns
    for pat in req_list:
        pat_glob = pat.replace('{*}', '*')
        found_any = False
        for p in extract_dir.iterdir() if extract_dir.exists() else []:
            try:
                if p.is_file() and fnmatch.fnmatch(p.name, pat_glob):
                    found_any = True
                    break
            except Exception:
                continue
        if not found_any:
            # also recursively
            for p in extract_dir.rglob('*'):
                try:
                    if p.is_file() and fnmatch.fnmatch(p.name, pat_glob):
                        found_any = True
                        break
                except Exception:
                    continue
        if not found_any:
            missing_files.append(pat)

    # gather tokens from parsed: prefer parsed['tokens'] but always also scan raw_sql for any token-like patterns
    import re
    tokens = []
    # tokens explicitly provided by parser (if any)
    if parsed.get('tokens'):
        tokens = list(parsed.get('tokens'))
    # also find any token-like occurrences in the raw SQL to catch SELECT-list tokens etc.
    raw = parsed.get('raw_sql', '') or ''
    extra = re.findall(r'([A-Za-z0-9_]+_\{\*\})', raw)
    for t in extra:
        if t not in tokens:
            tokens.append(t)

    token_to_table = {}
    for t in tokens:
        p = _find_psv_for_token(t, module)
        if p is None:
            token_to_table[t] = None
            missing_files.append(t)
            continue
        # ***** read PSV using safe reader *****
        try:
            df = _safe_read_psv(p)
        except Exception:
            df = pd.DataFrame()
        # table name: use file stem (without extension)
        table_name = p.stem.lower()
        # If df has zero columns, record it for RCA and add to missing_cols
        if not isinstance(df, pd.DataFrame) or df.shape[1] == 0:
            print(f"***** _load_psvs_for_sql: file '{p.name}' yielded zero columns; adding to missing_cols and inserting placeholder")
            missing_cols.append(p.name)
            # create placeholder so downstream still registers something in duckdb
            df = pd.DataFrame({'_diag_col': ['']})
        # ensure strings
        try:
            df = df.fillna('').astype(str)
        except Exception:
            df = df.astype(str)
        df_map[table_name] = df
        token_to_table[t] = table_name

    # Fallback: also load any remaining .psv files present in the extracted folder that weren't matched
    try:
        extract_dir = _module_to_extracted.get(module)
        if extract_dir and extract_dir.exists():
            for p in extract_dir.iterdir():
                try:
                    if not p.is_file():
                        continue
                    if not p.name.lower().endswith('.psv'):
                        continue
                    table_name = p.stem.lower()
                    if table_name in df_map:
                        continue
                    # ***** use safe reader here as well *****
                    try:
                        df = _safe_read_psv(p)
                    except Exception:
                        df = pd.DataFrame()
                    if not isinstance(df, pd.DataFrame) or df.shape[1] == 0:
                        print(f"***** _load_psvs_for_sql (fallback): file '{p.name}' yielded zero columns; adding to missing_cols and inserting placeholder")
                        missing_cols.append(p.name)
                        df = pd.DataFrame({'_diag_col': ['']})
                    df_map[table_name] = df.fillna('').astype(str)
                except Exception:
                    continue
    except Exception:
        pass
    parsed['token_to_table'] = token_to_table
    # dedupe missing_files
    missing_files = list(dict.fromkeys(missing_files))
    # dedupe missing_cols
    missing_cols = list(dict.fromkeys(missing_cols))
    return df_map, missing_files, missing_cols


def _execute_parsed_sql(parsed: Dict, df_map: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Register DataFrames into duckdb and execute the parsed SQL after replacing tokens with concrete table names.

    Returns resulting pandas DataFrame.

    ***** CHANGES: If any loaded DataFrame has zero columns, replace with placeholder column before registering.
    Also add diagnostics and explicit error when query returns zero columns to provide RCA. *****
    """
    if parsed is None or not parsed.get('raw_sql'):
        return pd.DataFrame()
    # token_to_table mapping
    token_to_table = parsed.get('token_to_table', {})
    final_sql = parsed['raw_sql']
    # Replace tokens in SQL with actual table names (filename stems)
    for token, tab in token_to_table.items():
        if tab:
            final_sql = final_sql.replace(token, tab)
            final_sql = final_sql.replace(token + '.psv', tab)

    # If any token-like placeholders remain (e.g. in SELECT), attempt to resolve them by matching df_map keys
    import re
    token_re = re.compile(r'([A-Za-z0-9_]+_\{\*\})')
    for m in token_re.finditer(final_sql):
        tok = m.group(1)
        if tok in token_to_table and token_to_table.get(tok):
            continue
        # build glob pattern from token and match against df_map keys (which are file stems)
        pat = tok.replace('{*}', '*')
        # drop any trailing .psv if present
        if pat.endswith('.psv'):
            pat = pat[:-4]
        matched_key = None
        for key in df_map.keys():
            try:
                if fnmatch.fnmatch(key, pat):
                    matched_key = key
                    break
            except Exception:
                continue
        if matched_key:
            final_sql = final_sql.replace(tok, matched_key)
            final_sql = final_sql.replace(tok + '.psv', matched_key)

    # create duckdb connection
    # ***** Ensure con is defined before try so finally can safely reference it *****
    con = None
    try:
        con = duckdb.connect(database=':memory:')
        # register dataframes
        for table_name, df in df_map.items():
            try:
                # If df has zero columns for any reason, replace with placeholder column
                if not isinstance(df, pd.DataFrame) or df.shape[1] == 0:
                    print(f"***** _execute_parsed_sql: table '{table_name}' has zero columns; registering placeholder column '_diag_col' *****")
                    placeholder = pd.DataFrame({'_diag_col': ['']})
                    con.register(table_name, placeholder)
                else:
                    con.register(table_name, df)
            except Exception as e:
                print(f"***** _execute_parsed_sql: failed to register '{table_name}' normally, attempting fallback. Error: {e}")
                try:
                    # Fallback: write as a temporary table using CREATE TABLE AS SELECT
                    # create a temporary pandas df in duckdb context
                    con.register('df', df)
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                except Exception as e2:
                    print(f"***** _execute_parsed_sql: fallback create table also failed for '{table_name}': {e2}")
        # execute
        # debug prints
        try:
            print('[DEBUG] token_to_table=', token_to_table)
            print('[DEBUG] df_map keys and shapes=')
            for k, v in df_map.items():
                try:
                    print(f'    - {k}: shape={getattr(v, "shape", None)} cols={list(v.columns)[:10] if isinstance(v, pd.DataFrame) else None}')
                except Exception:
                    print(f'    - {k}: <unable to print shape>')
            print('[DEBUG] final_sql snippet=', final_sql[:1000])
            try:
                tables = con.execute("SHOW TABLES").df()
                print('[DEBUG] duckdb SHOW TABLES:\n', tables)
            except Exception:
                pass
        except Exception:
            pass
        # execute
        res = con.execute(final_sql).df()
        # If result is empty of columns, raise clearly with diagnostics (RCA)
        if isinstance(res, pd.DataFrame) and res.shape[1] == 0:
            # collect helpful diagnostic information
            tbl_info = []
            for k, v in df_map.items():
                try:
                    tbl_info.append(f"{k}:shape={getattr(v,'shape',None)}")
                except Exception:
                    tbl_info.append(f"{k}:shape=UNKNOWN")
            diag_msg = (
                "DuckDB query returned a DataFrame with zero columns.\n"
                "Possible reasons: SQL selected no columns after token replacement, or source PSV files had no columns.\n"
                "Final SQL (truncated):\n" + final_sql[:2000] + "\n\n"
                "Registered tables summary:\n" + "\n".join(tbl_info)
            )
            print('***** _execute_parsed_sql RCA: ' + diag_msg)
            raise ValueError(diag_msg)
        # ensure all columns are strings as per requirement
        if not res.empty:
            res = res.fillna('').astype(str)
        return res
    finally:
        try:
            if con:
                con.close()
        except Exception:
            pass
