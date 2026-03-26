# monthly_master_report.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


# ---------------------- helpers ----------------------
def normalize_id(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower()


def coerce_score(series: pd.Series) -> pd.Series:
    """
    Accepts values like: "82%", "82.3", 0.82, "", None
    - Strips whitespace
    - Treats <=1.0 without % as fraction -> *100
    - Returns float rounded to 2 decimals (NaN for missing)
    """
    raw = series.astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    perc = raw.str.endswith("%", na=False)
    cleaned = raw.str.replace("%", "", regex=False)
    nums = pd.to_numeric(cleaned, errors="coerce")
    frac_mask = (nums <= 1.0) & (~perc)
    nums.loc[frac_mask] = nums.loc[frac_mask] * 100.0
    return nums.round(2)


def find_col(df: pd.DataFrame, candidates):
    lowmap = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lowmap:
            return lowmap[cand.lower()]
    want = [c.lower().replace(" ", "") for c in candidates]
    for c in df.columns:
        if c.lower().replace(" ", "") in want:
            return c
    return None


def coalesce_into(df: pd.DataFrame, target: str, *sources: str) -> None:
    if target not in df.columns:
        df[target] = pd.NA
    for s in sources:
        if s in df.columns:
            df[target] = df[target].fillna(df[s])


def choose_panorama_sheet(sheet_names, override: str | None) -> str:
    if override and override in sheet_names:
        return override
    for p in ("Course Accessibility", "Accessibility", "Courses"):
        if p in sheet_names:
            return p
    for sn in sheet_names:
        low = sn.lower()
        if "course" in low and ("access" in low or "panorama" in low):
            return sn
    return sheet_names[0]


def ensure_base_master_columns(df: pd.DataFrame) -> pd.DataFrame:
    base_cols = [
        "course_id",
        "Term",
        "Department id",
        "Department name",
        "Course code",
        "Course name",
        "Number of students",
    ]
    for c in base_cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def validate_report_month(report_month: str) -> str:
    """
    Expect 'YYYY-MM'. Raises ValueError if invalid.
    """
    try:
        datetime.strptime(report_month, "%Y-%m")
    except ValueError as e:
        raise ValueError("report_month must be in 'YYYY-MM' format, e.g., '2026-02'") from e
    return report_month


def _clean_dept_id_series(s: pd.Series) -> pd.Series:
    # Handles numeric dept ids stored as floats like 551.0
    return pd.to_numeric(s, errors='coerce').astype('Int64').astype(str)


def _coerce_students(series: pd.Series) -> pd.Series:
    """
    Convert Number of students to numeric. Returns float with NaN on bad values.
    Handles strings like "0", "0.0", "", None.
    """
    s = series.astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    s = s.str.replace(",", "", regex=False)
    s = s.str.replace(".0", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


# ---------------------- snapshot prep (current month only) ----------------------
def prepare_month_snapshot(
    ally_path: Path,
    pan_path: Path,
    term_filter: str | None,
    dept_ids_filter: list[str] | None,
    pan_sheet_override: str | None,
) -> pd.DataFrame:
    """
    Returns a canonical course-level snapshot for the current month.
    """
    ally_path = Path(ally_path)
    pan_path = Path(pan_path)
    if not ally_path.exists():
        raise FileNotFoundError(f"Missing Ally file: {ally_path}")
    if not pan_path.exists():
        raise FileNotFoundError(f"Missing Panorama file: {pan_path}")

    # Ally (csv or xlsx)
    if ally_path.suffix.lower() == ".csv":
        ally_df = pd.read_csv(ally_path, dtype=str)
    else:
        ally_xf = pd.ExcelFile(ally_path)
        ally_df = pd.read_excel(ally_xf, sheet_name=ally_xf.sheet_names[0], dtype=str)

    # Panorama (xlsx)
    pan_xf = pd.ExcelFile(pan_path)
    pan_sheet = choose_panorama_sheet(pan_xf.sheet_names, pan_sheet_override)
    pan_df = pd.read_excel(pan_xf, sheet_name=pan_sheet, dtype=str)

    # Prep Ally
    ALLY_ID_COL = find_col(ally_df, ["Course id", "Course ID"])
    ALLY_SCORE_COL = find_col(ally_df, ["Overall score", "Overall Score", "Ally score", "Ally Score"])
    if not ALLY_ID_COL or not ALLY_SCORE_COL:
        raise RuntimeError(f"Could not find Ally id/score columns. id={ALLY_ID_COL}, score={ALLY_SCORE_COL}")

    a = ally_df.rename(columns={ALLY_ID_COL: "course_id"}).copy()
    a["course_id"] = normalize_id(a["course_id"])
    a["Ally score"] = coerce_score(ally_df[ALLY_SCORE_COL])

    context_cols = [
        c
        for c in [
            "Term name", "Term Name", "Term", "Dept id", "Dept ID", "Department id",
            "Department name", "Dept name", "Course code", "Course Code",
            "Course name", "Course Name", "Number of students", "Number Of Students",
            "Number of Students", "Enrolled", "Prefix", "Number", "Section",
        ]
        if c in a.columns
    ]
    a = a[["course_id", "Ally score"] + context_cols]

    # Prep Panorama
    PAN_ID_COL = find_col(pan_df, ["Course ID", "Course id"])
    PAN_SCORE_COL = find_col(pan_df, ["Overall Accessibility Score", "Panorama Score", "Panorama score"])
    if not PAN_ID_COL or not PAN_SCORE_COL:
        raise RuntimeError(f"Could not find Panorama id/score columns. id={PAN_ID_COL}, score={PAN_SCORE_COL}")

    p = pan_df.rename(columns={PAN_ID_COL: "course_id"}).copy()
    p["course_id"] = normalize_id(p["course_id"])
    p["Panorama Score"] = coerce_score(p[PAN_SCORE_COL])
    p = p[
        ["course_id", "Panorama Score"]
        + [
            c
            for c in [
                "Term Name", "Term", "Course Name", "Course Code", "Department id",
                "Department name", "Dept id", "Dept name", "Number of students", "Enrolled",
            ]
            if c in p.columns
        ]
    ]

    # Merge Ally + Panorama for this month
    merged = pd.merge(a, p, on="course_id", how="outer")

    # Canonicalize text cols
    coalesce_into(merged, "Term name", "Term name", "Term Name", "Term")
    coalesce_into(merged, "Course name", "Course name", "Course Name")
    coalesce_into(merged, "Course code", "Course code", "Course Code")
    coalesce_into(merged, "Department id", "Department id", "Dept id", "Dept ID")
    coalesce_into(merged, "Department name", "Department name", "Dept name")
    coalesce_into(merged, "Number of students", "Number of students", "Number Of Students", "Number of Students", "Enrolled")

    for col in [
        "Term Name", "Term", "Course Name", "Course Code", "Dept id", "Dept ID",
        "Dept name", "Number Of Students", "Number of Students", "Enrolled",
    ]:
        if col in merged.columns and col not in (
            "Term name", "Course name", "Course code",
            "Department id", "Department name", "Number of students",
        ):
            merged.drop(columns=[col], inplace=True)

    if dept_ids_filter and "Department id" in merged.columns:
        wanted = {str(x).strip() for x in dept_ids_filter}
        dept_clean = _clean_dept_id_series(merged["Department id"])
        merged = merged[dept_clean.isin(wanted)]

    if term_filter and "Term name" in merged.columns:
        merged = merged[
            merged["Term name"].astype(str).str.strip().str.casefold() == term_filter.casefold()
        ]

    merged.rename(columns={"Term name": "Term"}, inplace=True)

    raw_cols = [
        c
        for c in (
            "Term", "Department id", "Department name", "course_id",
            "Course code", "Course name", "Number of students",
            "Ally score", "Panorama Score",
        )
        if c in merged.columns
    ]
    return merged[raw_cols]


# ---------------------- monthly master builder ----------------------
@dataclass(frozen=True)
class MonthlyMasterConfig:
    report_month: str
    reset_year: str | None = None


def build_monthly_master_report(
    prev_master: Path | None,
    ally_current: Path,
    pan_current: Path,
    output_path: Path,
    report_month: str,
    term_filter: str | None = None,
    dept_ids_filter: list[str] | None = None,
    pan_sheet_override: str | None = None,
    keep_only_prev_courses: bool = True,
    reset_to_year: str | None = None,
    exclude_zero_enrollment: bool = False,
) -> None:
    
    report_month = validate_report_month(report_month)
    year = report_month[:4]
    if reset_to_year is not None and reset_to_year != year:
        reset_to_year = year

    ally_col = f"Ally {report_month}"
    pan_col = f"Panorama {report_month}"

    # 1) Load or create master
    if prev_master is not None:
        prev_master = Path(prev_master)
        if not prev_master.exists():
            raise FileNotFoundError(f"Missing previous master file: {prev_master}")
        master_df = pd.read_excel(prev_master, dtype=object)
    else:
        master_df = pd.DataFrame()

    master_df = ensure_base_master_columns(master_df)
    master_df["course_id"] = normalize_id(master_df["course_id"])

    # 2) Build current month snapshot
    snap = prepare_month_snapshot(
        ally_path=Path(ally_current),
        pan_path=Path(pan_current),
        term_filter=term_filter,
        dept_ids_filter=dept_ids_filter,
        pan_sheet_override=pan_sheet_override,
    ).copy()
    snap["course_id"] = normalize_id(snap["course_id"])

    # Rename snapshot score cols to month cols
    rename_map = {}
    if "Ally score" in snap.columns:
        rename_map["Ally score"] = ally_col
    if "Panorama Score" in snap.columns:
        rename_map["Panorama Score"] = pan_col
    snap.rename(columns=rename_map, inplace=True)

    if ally_col not in snap.columns:
        snap[ally_col] = pd.NA
    if pan_col not in snap.columns:
        snap[pan_col] = pd.NA

    # 3) Merge into master
    how = "left" if (prev_master is not None and keep_only_prev_courses) else "outer"
    merged = pd.merge(
        master_df,
        snap,
        on="course_id",
        how=how,
        suffixes=("", "_newmeta"),
    )

    # 4) Coalesce base metadata
    base_meta = [
        "Term", "Department id", "Department name",
        "Course code", "Course name", "Number of students",
    ]
    for col in base_meta:
        newcol = f"{col}_newmeta"
        if newcol in merged.columns:
            merged[col] = merged[col].fillna(merged[newcol])
            merged.drop(columns=[newcol], inplace=True)

    # 4.5) Apply College filter to FINAL output
    if dept_ids_filter:
        if "Department id" not in merged.columns:
            raise RuntimeError("College filter requested but 'Department id' is missing in merged output.")
        wanted = {str(x).strip() for x in dept_ids_filter}
        dept_clean = _clean_dept_id_series(merged["Department id"])
        merged = merged[dept_clean.isin(wanted)]

    # 4.6) Exclude 0 enrollment rows
    if exclude_zero_enrollment:
        if "Number of students" in merged.columns:
            n = _coerce_students(merged["Number of students"])
            merged = merged[~(n == 0)]

    # 6) Enforce year reset
    if reset_to_year is not None:
        month_cols = [c for c in merged.columns if c.startswith("Ally ") or c.startswith("Panorama ")]
        for c in month_cols:
            try:
                c_year = c.split(" ", 1)[1].split("-", 1)[0]
            except Exception:
                continue
            if c_year != reset_to_year:
                merged.drop(columns=[c], inplace=True)

    # 7) Order columns
    base_cols = [
        "course_id", "Term", "Department id", "Department name",
        "Course code", "Course name", "Number of students",
    ]
    month_cols = sorted([c for c in merged.columns if c.startswith("Ally ") or c.startswith("Panorama ")])
    other_cols = [c for c in merged.columns if c not in set(base_cols + month_cols)]
    final_cols = [c for c in base_cols if c in merged.columns] + month_cols + other_cols
    merged = merged[final_cols]

    # ==========================================
    # 8) Generate summary sheet
    # ==========================================
    
    # Prep data for aggregation
    student_counts = _coerce_students(merged["Number of students"]).fillna(0)
    ally_series = pd.to_numeric(merged[ally_col], errors="coerce")
    pan_series = pd.to_numeric(merged[pan_col], errors="coerce")
    
    summary_df = merged.copy()
    summary_df["_ally_num"] = ally_series
    summary_df["_pan_num"] = pan_series
    summary_df["_students_num"] = student_counts
    summary_df["Department name"] = summary_df["Department name"].fillna("Unknown")
    
    # Group by Department and aggregate
    summary = summary_df.groupby("Department name").agg(
        Average_of_Ally=("_ally_num", "mean"),
        Average_of_Pan=("_pan_num", "mean"),
        Total_Students=("_students_num", "sum"),
        Total_Courses=("course_id", "count")
    ).reset_index()
    
    # Rename columns to perfectly match the Dean file layout
    summary.rename(columns={
        "Department name": "Department",
        "Average_of_Ally": "Average of Ally Score",
        "Average_of_Pan": "Average of Panorama Score",
        "Total_Students": "Total Number of Students",
        "Total_Courses": "Total Number of Courses"
    }, inplace=True)
    
    summary["Difference in Scores"] = summary["Average of Panorama Score"] - summary["Average of Ally Score"]
    
    # Calculate overall metrics for the Grand Total row
    overall_ally = summary_df["_ally_num"].mean()
    overall_pan = summary_df["_pan_num"].mean()
    overall_students = summary_df["_students_num"].sum()
    overall_courses = len(summary_df)
    overall_diff = overall_pan - overall_ally if pd.notnull(overall_pan) and pd.notnull(overall_ally) else pd.NA
    
    grand_total_row = pd.DataFrame([{
        "Department": "Grand Total",
        "Average of Ally Score": overall_ally,
        "Average of Panorama Score": overall_pan,
        "Total Number of Students": overall_students,
        "Total Number of Courses": overall_courses,
        "Difference in Scores": overall_diff
    }])
    
    # Append the grand total
    summary = pd.concat([summary, grand_total_row], ignore_index=True)
    
    # Round to 2 decimal places to match the presentation format
    for col in ["Average of Ally Score", "Average of Panorama Score", "Difference in Scores"]:
        summary[col] = summary[col].round(2)


    # ==========================================
    # 9) Write to excel
    # ==========================================
    output_path = Path(output_path)
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        
        # Write both sheets
        summary.to_excel(writer, sheet_name="Summary", index=False)
        merged.to_excel(writer, sheet_name="Courses", index=False)

        wb = writer.book
        ws_summary = writer.sheets["Summary"]
        ws_courses = writer.sheets["Courses"]

        # --- Formatting: Summary Sheet ---
        # Make the header look clean and bold
        header_fmt = wb.add_format({'bold': True, 'bottom': 1, 'text_wrap': True, 'align': 'center'})
        for col_num, value in enumerate(summary.columns.values):
            ws_summary.write(0, col_num, value, header_fmt)
            
        # Set column widths
        ws_summary.set_column(0, 0, 35) # Department
        ws_summary.set_column(1, 2, 22) # Averages
        ws_summary.set_column(3, 4, 22) # Totals
        ws_summary.set_column(5, 5, 20) # Difference
        
        # Float format for scores
        float_fmt = wb.add_format({'num_format': '0.00'})
        ws_summary.set_column(1, 2, 22, float_fmt)
        ws_summary.set_column(5, 5, 20, float_fmt)
        
        # Bold the Grand Total Row
        bold_row_fmt = wb.add_format({'bold': True})
        bold_float_fmt = wb.add_format({'bold': True, 'num_format': '0.00'})
        gt_row_idx = len(summary) # Last row of the data (len(df) accounts for header in xlsxwriter)
        
        ws_summary.write(gt_row_idx, 0, summary.iloc[-1]["Department"], bold_row_fmt)
        ws_summary.write_number(gt_row_idx, 1, summary.iloc[-1]["Average of Ally Score"], bold_float_fmt)
        ws_summary.write_number(gt_row_idx, 2, summary.iloc[-1]["Average of Panorama Score"], bold_float_fmt)
        ws_summary.write_number(gt_row_idx, 3, summary.iloc[-1]["Total Number of Students"], bold_row_fmt)
        ws_summary.write_number(gt_row_idx, 4, summary.iloc[-1]["Total Number of Courses"], bold_row_fmt)
        ws_summary.write_number(gt_row_idx, 5, summary.iloc[-1]["Difference in Scores"], bold_float_fmt)


        # --- Formatting: Courses Sheet ---
        ws_courses.freeze_panes(1, 0)

        for i, col in enumerate(merged.columns):
            width = min(max(12, len(str(col)) + 2), 40)
            ws_courses.set_column(i, i, width)

        # Conditional formatting logic
        f_red = wb.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
        f_orange = wb.add_format({"bg_color": "#FFEB9C", "font_color": "#9C6500"})
        f_green = wb.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"})
        f_gold = wb.add_format({"bg_color": "#FFD966"})

        first_row = 1
        last_row = len(merged)

        for i, col in enumerate(merged.columns):
            low = str(col).lower()
            if low.startswith("ally "):
                ws_courses.conditional_format(first_row, i, last_row, i, {"type": "cell", "criteria": "<=", "value": 33, "format": f_red})
                ws_courses.conditional_format(first_row, i, last_row, i, {"type": "cell", "criteria": "between", "minimum": 34, "maximum": 66, "format": f_orange})
                ws_courses.conditional_format(first_row, i, last_row, i, {"type": "cell", "criteria": ">=", "value": 67, "format": f_green})
            elif low.startswith("panorama "):
                ws_courses.conditional_format(first_row, i, last_row, i, {"type": "cell", "criteria": "<=", "value": 30, "format": f_red})
                ws_courses.conditional_format(first_row, i, last_row, i, {"type": "cell", "criteria": "between", "minimum": 30.01, "maximum": 80, "format": f_gold})
                ws_courses.conditional_format(first_row, i, last_row, i, {"type": "cell", "criteria": ">=", "value": 80.01, "format": f_green})

    print(f"Wrote monthly master report to {output_path}")


# ---------------------- parse arguments ----------------------
def parse_args():
    ap = argparse.ArgumentParser(description="Monthly Master Accessibility Report Builder")
    ap.add_argument("--prev-master", default=None, type=Path, help="Previous month master (.xlsx). Omit for first month.")
    ap.add_argument("--ally", required=True, type=Path, help="Current month Ally (csv/xlsx)")
    ap.add_argument("--pan", required=True, type=Path, help="Current month Panorama (xlsx)")
    ap.add_argument("--out", required=True, type=Path, help="Output master Excel path")
    ap.add_argument("--month", required=True, help="Report month in YYYY-MM format (e.g., 2026-02)")
    ap.add_argument("--term", default=None, help="Optional exact term filter")
    ap.add_argument("--dept-ids", default=None, help="Optional comma-separated Department IDs (e.g., 551,629,630)")
    ap.add_argument("--exclude-zero", action="store_true", help="Exclude courses where Number of students == 0")
    ap.add_argument("--pan-sheet", default=None, help="Optional Panorama sheet override")
    ap.add_argument("--keep-only-prev", action="store_true", help="Keep only courses from prev master (LEFT join)")
    ap.add_argument("--reset-year", default=None, help="Drop month columns outside this year (e.g., 2026)")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()

    dept_ids = [x.strip() for x in args.dept_ids.split(",")] if args.dept_ids else None

    build_monthly_master_report(
        prev_master=args.prev_master,
        ally_current=args.ally,
        pan_current=args.pan,
        output_path=args.out,
        report_month=args.month,
        term_filter=args.term,
        dept_ids_filter=dept_ids,
        pan_sheet_override=args.pan_sheet,
        keep_only_prev_courses=args.keep_only_prev,
        reset_to_year=args.reset_year,
        exclude_zero_enrollment=args.exclude_zero,
    )
