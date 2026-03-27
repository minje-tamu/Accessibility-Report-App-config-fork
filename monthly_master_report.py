# monthly_master_report.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import numpy as np

import pandas as pd


# ---------------------- helpers ----------------------
def normalize_id(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower()


def coerce_score(series: pd.Series) -> pd.Series:
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
        "course_id", "Term", "Department id", "Department name",
        "Course code", "Course name", "Number of students",
    ]
    for c in base_cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def validate_report_month(report_month: str) -> str:
    try:
        datetime.strptime(report_month, "%Y-%m")
    except ValueError as e:
        raise ValueError("report_month must be in 'YYYY-MM' format, e.g., '2026-02'") from e
    return report_month


def _clean_dept_id_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors='coerce').astype('Int64').astype(str)


def _coerce_students(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    s = s.str.replace(",", "", regex=False)
    s = s.str.replace(".0", "", regex=False)
    return pd.to_numeric(s, errors="coerce")

def format_month_header(col_name: str) -> str:
    """Converts 'Ally 2026-02' to 'February Ally Score'"""
    if not col_name:
        return ""
    parts = col_name.split(" ", 1)
    if len(parts) == 2:
        try:
            dt = datetime.strptime(parts[1], "%Y-%m")
            month_str = dt.strftime("%B")
            return f"{month_str} {parts[0]} Score"
        except Exception:
            pass
    return col_name


# ---------------------- snapshot prep (current month only) ----------------------
def prepare_month_snapshot(
    ally_path: Path,
    pan_path: Path,
    term_filter: str | None,
    dept_ids_filter: list[str] | None,
    pan_sheet_override: str | None,
) -> pd.DataFrame:
    ally_path = Path(ally_path)
    pan_path = Path(pan_path)
    
    if ally_path.suffix.lower() == ".csv":
        ally_df = pd.read_csv(ally_path, dtype=str)
    else:
        ally_xf = pd.ExcelFile(ally_path)
        ally_df = pd.read_excel(ally_xf, sheet_name=ally_xf.sheet_names[0], dtype=str)

    pan_xf = pd.ExcelFile(pan_path)
    pan_sheet = choose_panorama_sheet(pan_xf.sheet_names, pan_sheet_override)
    pan_df = pd.read_excel(pan_xf, sheet_name=pan_sheet, dtype=str)

    ALLY_ID_COL = find_col(ally_df, ["Course id", "Course ID"])
    ALLY_SCORE_COL = find_col(ally_df, ["Overall score", "Overall Score", "Ally score", "Ally Score"])
    
    a = ally_df.rename(columns={ALLY_ID_COL: "course_id"}).copy()
    a["course_id"] = normalize_id(a["course_id"])
    a["Ally score"] = coerce_score(ally_df[ALLY_SCORE_COL])

    context_cols = [c for c in ["Term name", "Term Name", "Term", "Dept id", "Dept ID", "Department id", "Department name", "Dept name", "Course code", "Course Code", "Course name", "Course Name", "Number of students", "Number Of Students", "Number of Students", "Enrolled", "Prefix", "Number", "Section"] if c in a.columns]
    a = a[["course_id", "Ally score"] + context_cols]

    PAN_ID_COL = find_col(pan_df, ["Course ID", "Course id"])
    PAN_SCORE_COL = find_col(pan_df, ["Overall Accessibility Score", "Panorama Score", "Panorama score"])
    
    p = pan_df.rename(columns={PAN_ID_COL: "course_id"}).copy()
    p["course_id"] = normalize_id(p["course_id"])
    p["Panorama Score"] = coerce_score(p[PAN_SCORE_COL])
    p = p[["course_id", "Panorama Score"] + [c for c in ["Term Name", "Term", "Course Name", "Course Code", "Department id", "Department name", "Dept id", "Dept name", "Number of students", "Enrolled"] if c in p.columns]]

    merged = pd.merge(a, p, on="course_id", how="outer")

    coalesce_into(merged, "Term name", "Term name", "Term Name", "Term")
    coalesce_into(merged, "Course name", "Course name", "Course Name")
    coalesce_into(merged, "Course code", "Course code", "Course Code")
    coalesce_into(merged, "Department id", "Department id", "Dept id", "Dept ID")
    coalesce_into(merged, "Department name", "Department name", "Dept name")
    coalesce_into(merged, "Number of students", "Number of students", "Number Of Students", "Number of Students", "Enrolled")

    for col in ["Term Name", "Term", "Course Name", "Course Code", "Dept id", "Dept ID", "Dept name", "Number Of Students", "Number of Students", "Enrolled"]:
        if col in merged.columns and col not in ("Term name", "Course name", "Course code", "Department id", "Department name", "Number of students"):
            merged.drop(columns=[col], inplace=True)

    if dept_ids_filter and "Department id" in merged.columns:
        wanted = {str(x).strip() for x in dept_ids_filter}
        dept_clean = _clean_dept_id_series(merged["Department id"])
        merged = merged[dept_clean.isin(wanted)]

    if term_filter and "Term name" in merged.columns:
        merged = merged[merged["Term name"].astype(str).str.strip().str.casefold() == term_filter.casefold()]

    merged.rename(columns={"Term name": "Term"}, inplace=True)

    raw_cols = [c for c in ("Term", "Department id", "Department name", "course_id", "Course code", "Course name", "Number of students", "Ally score", "Panorama Score") if c in merged.columns]
    return merged[raw_cols]


# ---------------------- monthly master builder ----------------------
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
    college_name: str = "All Colleges",
) -> None:
    
    report_month = validate_report_month(report_month)
    year = report_month[:4]
    if reset_to_year is not None and reset_to_year != year:
        reset_to_year = year

    ally_col = f"Ally {report_month}"
    pan_col = f"Panorama {report_month}"

    if prev_master is not None:
        prev_master = Path(prev_master)
        master_df = pd.read_excel(prev_master, dtype=object)
    else:
        master_df = pd.DataFrame()

    master_df = ensure_base_master_columns(master_df)
    master_df["course_id"] = normalize_id(master_df["course_id"])

    snap = prepare_month_snapshot(Path(ally_current), Path(pan_current), term_filter, dept_ids_filter, pan_sheet_override).copy()
    snap["course_id"] = normalize_id(snap["course_id"])

    rename_map = {}
    if "Ally score" in snap.columns:
        rename_map["Ally score"] = ally_col
    if "Panorama Score" in snap.columns:
        rename_map["Panorama Score"] = pan_col
    snap.rename(columns=rename_map, inplace=True)

    if ally_col not in snap.columns: snap[ally_col] = pd.NA
    if pan_col not in snap.columns: snap[pan_col] = pd.NA

    how = "left" if (prev_master is not None and keep_only_prev_courses) else "outer"
    merged = pd.merge(master_df, snap, on="course_id", how=how, suffixes=("", "_newmeta"))

    base_meta = ["Term", "Department id", "Department name", "Course code", "Course name", "Number of students"]
    for col in base_meta:
        newcol = f"{col}_newmeta"
        if newcol in merged.columns:
            merged[col] = merged[col].fillna(merged[newcol])
            merged.drop(columns=[newcol], inplace=True)

    if dept_ids_filter:
        wanted = {str(x).strip() for x in dept_ids_filter}
        dept_clean = _clean_dept_id_series(merged["Department id"])
        merged = merged[dept_clean.isin(wanted)]

    if exclude_zero_enrollment:
        if "Number of students" in merged.columns:
            n = _coerce_students(merged["Number of students"])
            merged = merged[~(n == 0)]

    if reset_to_year is not None:
        month_cols = [c for c in merged.columns if c.startswith("Ally ") or c.startswith("Panorama ")]
        for c in month_cols:
            try:
                if c.split(" ", 1)[1].split("-", 1)[0] != reset_to_year:
                    merged.drop(columns=[c], inplace=True)
            except Exception: continue

    base_cols = ["course_id", "Term", "Department id", "Department name", "Course code", "Course name", "Number of students"]
    month_cols = sorted([c for c in merged.columns if c.startswith("Ally ") or c.startswith("Panorama ")])
    other_cols = [c for c in merged.columns if c not in set(base_cols + month_cols)]
    merged = merged[[c for c in base_cols if c in merged.columns] + month_cols + other_cols]

    # ==========================================
    # GENERATE DEAN SUMMARY SHEET MATH
    # ==========================================
    ally_cols_sorted = sorted([c for c in merged.columns if c.startswith("Ally ")])
    pan_cols_sorted = sorted([c for c in merged.columns if c.startswith("Panorama ")])

    curr_ally_col = ally_cols_sorted[-1] if ally_cols_sorted else None
    prev_ally_col = ally_cols_sorted[-2] if len(ally_cols_sorted) > 1 else None
    
    curr_pan_col = pan_cols_sorted[-1] if pan_cols_sorted else None
    prev_pan_col = pan_cols_sorted[-2] if len(pan_cols_sorted) > 1 else None

    # Calculate Net Changes for the Courses tab
    if curr_ally_col and prev_ally_col:
        merged["Net Change (Ally)"] = pd.to_numeric(merged[curr_ally_col], errors="coerce") - pd.to_numeric(merged[prev_ally_col], errors="coerce")
    else:
        merged["Net Change (Ally)"] = pd.NA

    if curr_pan_col and prev_pan_col:
        merged["Net Change (Panorama)"] = pd.to_numeric(merged[curr_pan_col], errors="coerce") - pd.to_numeric(merged[prev_pan_col], errors="coerce")
    else:
        merged["Net Change (Panorama)"] = pd.NA

    student_counts = _coerce_students(merged["Number of students"]).fillna(0)
    summary_df = merged.copy()
    summary_df["_students_num"] = student_counts
    summary_df["Department name"] = summary_df["Department name"].fillna("Unknown")
    
    # Extract true numeric values (leave NaN as NaN)
    for c in [curr_ally_col, prev_ally_col, curr_pan_col, prev_pan_col]:
        if c:
            summary_df[f"{c}_num"] = pd.to_numeric(summary_df[c], errors="coerce")

    # Only count a course's students toward the denominator if the course ACTUALLY has a score for that specific metric
    summary_df["_a_curr_valid_students"] = np.where(summary_df[f"{curr_ally_col}_num"].notna(), student_counts, 0) if curr_ally_col else 0
    summary_df["_a_prev_valid_students"] = np.where(summary_df[f"{prev_ally_col}_num"].notna(), student_counts, 0) if prev_ally_col else 0
    summary_df["_p_curr_valid_students"] = np.where(summary_df[f"{curr_pan_col}_num"].notna(), student_counts, 0) if curr_pan_col else 0
    summary_df["_p_prev_valid_students"] = np.where(summary_df[f"{prev_pan_col}_num"].notna(), student_counts, 0) if prev_pan_col else 0

    # Calculate Weights (Score * Valid Students)
    summary_df["_a_curr_w"] = (summary_df[f"{curr_ally_col}_num"].fillna(0) * summary_df["_a_curr_valid_students"]) if curr_ally_col else 0
    summary_df["_a_prev_w"] = (summary_df[f"{prev_ally_col}_num"].fillna(0) * summary_df["_a_prev_valid_students"]) if prev_ally_col else 0
    summary_df["_p_curr_w"] = (summary_df[f"{curr_pan_col}_num"].fillna(0) * summary_df["_p_curr_valid_students"]) if curr_pan_col else 0
    summary_df["_p_prev_w"] = (summary_df[f"{prev_pan_col}_num"].fillna(0) * summary_df["_p_prev_valid_students"]) if prev_pan_col else 0

    # Group and aggregate
    summary = summary_df.groupby("Department name").agg(
        Total_Students=("_students_num", "sum"), 
        Total_Courses=("course_id", "count"),    
        A_Curr_W=("_a_curr_w", "sum"),
        A_Prev_W=("_a_prev_w", "sum"),
        P_Curr_W=("_p_curr_w", "sum"),
        P_Prev_W=("_p_prev_w", "sum"),
        A_Curr_Valid_Students=("_a_curr_valid_students", "sum"),
        A_Prev_Valid_Students=("_a_prev_valid_students", "sum"),
        P_Curr_Valid_Students=("_p_curr_valid_students", "sum"),
        P_Prev_Valid_Students=("_p_prev_valid_students", "sum")
    ).reset_index()

    # Final Cumulative Calculation (Total Weight / VALID Student Denominator)
    summary["A_Curr_Score"] = np.where(summary["A_Curr_Valid_Students"] > 0, summary["A_Curr_W"] / summary["A_Curr_Valid_Students"], pd.NA)
    summary["A_Prev_Score"] = np.where(summary["A_Prev_Valid_Students"] > 0, summary["A_Prev_W"] / summary["A_Prev_Valid_Students"], pd.NA)
    summary["P_Curr_Score"] = np.where(summary["P_Curr_Valid_Students"] > 0, summary["P_Curr_W"] / summary["P_Curr_Valid_Students"], pd.NA)
    summary["P_Prev_Score"] = np.where(summary["P_Prev_Valid_Students"] > 0, summary["P_Prev_W"] / summary["P_Prev_Valid_Students"], pd.NA)

    # Calculate Differences
    summary["Diff_Ally"] = summary["A_Curr_Score"] - summary["A_Prev_Score"]
    summary["Diff_Pan"] = summary["P_Curr_Score"] - summary["P_Prev_Score"]

    # Calculate Overall Grand Totals (Using the same strict valid student logic)
    overall_students = summary_df["_students_num"].sum()
    
    overall_a_curr_valid = summary_df["_a_curr_valid_students"].sum()
    overall_a_prev_valid = summary_df["_a_prev_valid_students"].sum()
    overall_p_curr_valid = summary_df["_p_curr_valid_students"].sum()
    overall_p_prev_valid = summary_df["_p_prev_valid_students"].sum()

    overall_row_data = {
        "Department name": "Overall",
        "Total_Students": overall_students,
        "Total_Courses": len(summary_df),
        "A_Curr_Score": summary_df["_a_curr_w"].sum() / overall_a_curr_valid if overall_a_curr_valid > 0 else pd.NA,
        "A_Prev_Score": summary_df["_a_prev_w"].sum() / overall_a_prev_valid if overall_a_prev_valid > 0 else pd.NA,
        "P_Curr_Score": summary_df["_p_curr_w"].sum() / overall_p_curr_valid if overall_p_curr_valid > 0 else pd.NA,
        "P_Prev_Score": summary_df["_p_prev_w"].sum() / overall_p_prev_valid if overall_p_prev_valid > 0 else pd.NA,
    }
    overall_row_data["Diff_Ally"] = overall_row_data["A_Curr_Score"] - overall_row_data["A_Prev_Score"] if pd.notna(overall_row_data["A_Curr_Score"]) and pd.notna(overall_row_data["A_Prev_Score"]) else pd.NA
    overall_row_data["Diff_Pan"] = overall_row_data["P_Curr_Score"] - overall_row_data["P_Prev_Score"] if pd.notna(overall_row_data["P_Curr_Score"]) and pd.notna(overall_row_data["P_Prev_Score"]) else pd.NA

    overall_row = pd.DataFrame([overall_row_data])
    summary = pd.concat([overall_row, summary], ignore_index=True)

    # Build the final column layout dynamically
    final_cols_map = {"Department name": "Department"}
    if prev_ally_col: final_cols_map["A_Prev_Score"] = format_month_header(prev_ally_col)
    if curr_ally_col: final_cols_map["A_Curr_Score"] = format_month_header(curr_ally_col)
    if prev_ally_col and curr_ally_col: final_cols_map["Diff_Ally"] = "Difference in Scores (Ally)"
    
    if prev_pan_col: final_cols_map["P_Prev_Score"] = format_month_header(prev_pan_col)
    if curr_pan_col: final_cols_map["P_Curr_Score"] = format_month_header(curr_pan_col)
    if prev_pan_col and curr_pan_col: final_cols_map["Diff_Pan"] = "Difference in Scores (Pan)"
    
    final_cols_map["Total_Students"] = "Total Number of Students"
    final_cols_map["Total_Courses"] = "Total Number of Courses"

    summary = summary.rename(columns=final_cols_map)[list(final_cols_map.values())]

    # Convert to decimals so Excel natively formats as %
    pct_cols = [c for c in summary.columns if "Score" in c]
    for c in pct_cols:
        summary[c] = summary[c] / 100.0

    # ==========================================
    # WRITE TO EXCEL
    # ==========================================
    output_path = Path(output_path)
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        
        summary.to_excel(writer, sheet_name="Summary", startrow=8, index=False, header=False)
        merged.to_excel(writer, sheet_name="Courses", index=False)

        wb = writer.book
        ws_summary = writer.sheets["Summary"]
        ws_courses = writer.sheets["Courses"]

        # --- Formatting: Summary Sheet ---
        bold_fmt = wb.add_format({'bold': True})
        header_fmt = wb.add_format({'bold': True, 'bottom': 1, 'text_wrap': True, 'align': 'center'})
        percent_fmt = wb.add_format({'num_format': '0.0%'})
        percent_bold_fmt = wb.add_format({'bold': True, 'num_format': '0.0%'})
        
        ws_summary.write(0, 0, "College:", bold_fmt)
        ws_summary.write(0, 1, college_name)
        ws_summary.write(1, 0, "Term:", bold_fmt)
        ws_summary.write(1, 1, term_filter if term_filter else "All Terms")
        ws_summary.write(2, 0, "0 Enrolled Courses Excluded:", bold_fmt)
        ws_summary.write(2, 1, "Yes" if exclude_zero_enrollment else "No")

        actual_headers = [c.replace(" (Ally)", "").replace(" (Pan)", "") for c in summary.columns]
        for col_num, value in enumerate(actual_headers):
            ws_summary.write(7, col_num, value, header_fmt)

        ws_summary.set_column(0, 0, 35) 
        
        col_idx = 1
        for col_name in summary.columns[1:]:
            if "Score" in col_name:
                ws_summary.set_column(col_idx, col_idx, 22, percent_fmt)
            else:
                ws_summary.set_column(col_idx, col_idx, 22)
            col_idx += 1

        ws_summary.write(8, 0, summary.iloc[0]["Department"], bold_fmt)
        col_idx = 1
        for col_name in summary.columns[1:]:
            val = summary.iloc[0][col_name]
            if "Score" in col_name:
                ws_summary.write_number(8, col_idx, val if pd.notna(val) else 0, percent_bold_fmt)
            else:
                ws_summary.write_number(8, col_idx, val if pd.notna(val) else 0, bold_fmt)
            col_idx += 1


        # --- Formatting: Courses Sheet ---
        ws_courses.freeze_panes(1, 0)
        for i, col in enumerate(merged.columns):
            width = min(max(12, len(str(col)) + 2), 40)
            ws_courses.set_column(i, i, width)

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

    print(f"✅ Wrote monthly master report to {output_path}")

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
