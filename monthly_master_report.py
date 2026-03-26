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
    college_name: str = "All Colleges", # New parameter for the Dean layout
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
    # GENERATE DEAN SUMMARY SHEET
    # ==========================================
    student_counts = _coerce_students(merged["Number of students"]).fillna(0)
    ally_series = pd.to_numeric(merged[ally_col], errors="coerce")
    pan_series = pd.to_numeric(merged[pan_col], errors="coerce")
    
    summary_df = merged.copy()
    summary_df["_ally_num"] = ally_series
    summary_df["_pan_num"] = pan_series
    summary_df["_students_num"] = student_counts
    summary_df["Department name"] = summary_df["Department name"].fillna("Unknown")
    
    summary = summary_df.groupby("Department name").agg(
        Ally_Score=("_ally_num", "mean"),
        Panorama_Score=("_pan_num", "mean"),
        Total_Students=("_students_num", "sum"),
        Total_Courses=("course_id", "count")
    ).reset_index()
    
    summary.rename(columns={
        "Department name": "Department",
        "Ally_Score": "Ally Score",
        "Panorama_Score": "Panorama Score",
        "Total_Students": "Total Number of Students",
        "Total_Courses": "Total Number of Courses"
    }, inplace=True)
    
    summary["Difference in Scores"] = summary["Panorama Score"] - summary["Ally Score"]
    
    overall_ally = summary_df["_ally_num"].mean()
    overall_pan = summary_df["_pan_num"].mean()
    overall_students = summary_df["_students_num"].sum()
    overall_courses = len(summary_df)
    overall_diff = overall_pan - overall_ally if pd.notnull(overall_pan) and pd.notnull(overall_ally) else pd.NA
    
    overall_row = pd.DataFrame([{
        "Department": "Overall",
        "Ally Score": overall_ally,
        "Panorama Score": overall_pan,
        "Total Number of Students": overall_students,
        "Total Number of Courses": overall_courses,
        "Difference in Scores": overall_diff
    }])
    
    summary = pd.concat([overall_row, summary], ignore_index=True)
    
    # Divide by 100 so Excel natively formats them as percentages (e.g. 85%)
    for col in ["Ally Score", "Panorama Score", "Difference in Scores"]:
        summary[col] = summary[col] / 100.0

    # ==========================================
    # WRITE TO EXCEL
    # ==========================================
    output_path = Path(output_path)
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        
        # Write summary table starting at Row 21 (Index 20)
        summary.to_excel(writer, sheet_name="Summary", startrow=20, index=False)
        merged.to_excel(writer, sheet_name="Courses", index=False)

        wb = writer.book
        ws_summary = writer.sheets["Summary"]
        ws_courses = writer.sheets["Courses"]

        # --- Formatting: Summary Sheet ---
        bold_fmt = wb.add_format({'bold': True})
        header_fmt = wb.add_format({'bold': True, 'bottom': 1, 'text_wrap': True, 'align': 'center'})
        percent_fmt = wb.add_format({'num_format': '0%'})
        percent_bold_fmt = wb.add_format({'bold': True, 'num_format': '0%'})
        
        ws_summary.set_column(0, 0, 35) # Department
        ws_summary.set_column(1, 2, 22, percent_fmt) # Scores
        ws_summary.set_column(3, 4, 22) # Totals
        ws_summary.set_column(5, 5, 20, percent_fmt) # Difference

        # Apply formatting to headers
        for col_num, value in enumerate(summary.columns.values):
            ws_summary.write(20, col_num, value, header_fmt)

        # Bold the Overall row (Row 22)
        ws_summary.write(21, 0, summary.iloc[0]["Department"], bold_fmt)
        ws_summary.write_number(21, 1, summary.iloc[0]["Ally Score"], percent_bold_fmt)
        ws_summary.write_number(21, 2, summary.iloc[0]["Panorama Score"], percent_bold_fmt)
        ws_summary.write_number(21, 3, summary.iloc[0]["Total Number of Students"], bold_fmt)
        ws_summary.write_number(21, 4, summary.iloc[0]["Total Number of Courses"], bold_fmt)
        ws_summary.write_number(21, 5, summary.iloc[0]["Difference in Scores"], percent_bold_fmt)

        # --- Pre-Row 22 Dashboard Metadata ---
        ws_summary.write(1, 1, "College:", bold_fmt)
        ws_summary.write(1, 2, college_name)
        
        ws_summary.write(2, 1, "Term:", bold_fmt)
        ws_summary.write(2, 2, term_filter if term_filter else "All Terms")
        
        ws_summary.write(3, 1, "0 Enrolled Courses Excluded:", bold_fmt)
        ws_summary.write(3, 2, "Yes" if exclude_zero_enrollment else "No")

        # --- Dashboard Visuals (Doughnut Charts) ---
        # Write hidden remainders needed for doughnut gauge charts
        ws_summary.write_formula('H22', '=IF(B22="","",1-B22)')
        ws_summary.write_formula('I22', '=IF(C22="","",1-C22)')
        ws_summary.set_column('H:I', None, None, {'hidden': True})

        chart_ally = wb.add_chart({'type': 'doughnut'})
        chart_ally.add_series({
            'name': 'Ally Overall',
            'values': '=(Summary!$B$22,Summary!$H$22)',
            'points': [{'fill': {'color': '#C6EFCE'}}, {'fill': {'color': '#F2F2F2'}}]
        })
        chart_ally.set_hole_size(60)
        chart_ally.set_title({'name': 'Overall Ally Score'})
        ws_summary.insert_chart('B6', chart_ally, {'x_scale': 0.8, 'y_scale': 0.8})

        chart_pan = wb.add_chart({'type': 'doughnut'})
        chart_pan.add_series({
            'name': 'Panorama Overall',
            'values': '=(Summary!$C$22,Summary!$I$22)',
            'points': [{'fill': {'color': '#C6EFCE'}}, {'fill': {'color': '#F2F2F2'}}]
        })
        chart_pan.set_hole_size(60)
        chart_pan.set_title({'name': 'Overall Panorama Score'})
        ws_summary.insert_chart('D6', chart_pan, {'x_scale': 0.8, 'y_scale': 0.8})

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

    print(f"Wrote monthly master report to {output_path}")

# ... CLI block remains untouched ...
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
