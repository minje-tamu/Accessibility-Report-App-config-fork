# app.py (Streamlit) — Monthly Master Builder
from pathlib import Path
import tempfile
import os
import json

import streamlit as st

from monthly_master_report import build_monthly_master_report

# Load dept id config file
with open("config.json", "r") as config_file:
    config_data = json.load(config_file)
    SCHOOL_DEPT_IDS = config_data["SCHOOL_DEPT_IDS"]


def save_uploaded_file(uploaded_file) -> Path:
    """Save Streamlit UploadedFile to a temp file and return Path."""
    suffix = Path(uploaded_file.name).suffix or ""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(uploaded_file.getbuffer())
    tmp.close()
    return Path(tmp.name)


st.set_page_config(page_title="Monthly Accessibility Master Report", layout="wide")
st.title("Monthly Accessibility Master Report (Yearly Reset)")

st.markdown("### Upload files")

col1, col2 = st.columns(2)

with col1:
    prev_master_file = st.file_uploader(
        "Previous month master (.xlsx) — optional for first month",
        type=["xlsx"],
        key="prev_master",
    )
    ally_file = st.file_uploader("Current month Ally (csv/xlsx)", type=["csv", "xlsx"], key="ally")

with col2:
    pan_file = st.file_uploader("Current month Panorama (.xlsx)", type=["xlsx"], key="pan")

st.markdown("---")
st.markdown("### Settings")

report_month = st.text_input("Report month (YYYY-MM)", value="2026-02")

keep_only_prev = st.checkbox(
    "Keep only courses from previous master (recommended for your workflow)",
    value=False,
)

reset_year = st.text_input(
    "Reset year (optional) — drops month columns outside this year (e.g., 2026)",
    value="",
)

# ✅ College dropdown
school_options = ["All Colleges"] + list(SCHOOL_DEPT_IDS.keys())
selected_school = st.selectbox("College filter (optional)", school_options, index=0)

dept_ids_filter = None
if selected_school != "All Colleges":
    dept_ids_filter = SCHOOL_DEPT_IDS[selected_school]

term_filter = st.text_input("Term filter (optional)", value="")
pan_sheet = st.text_input("Panorama sheet override (optional)", value="")

# ✅ Zero enrollment dropdown
zero_opts = ["No", "Yes"]
exclude_zero_choice = st.selectbox("Exclude 0 enrollment courses?", zero_opts, index=0)
exclude_zero_enrollment = (exclude_zero_choice == "Yes")

# Verify config DEPT loading
with st.expander("View Department IDs"):
    st.markdown("List loaded from config.json")
    st.json(SCHOOL_DEPT_IDS)

st.markdown("---")
generate = st.button("Generate Master Excel", type="primary")

if generate:
    if not (ally_file and pan_file):
        st.error("Please upload the current month Ally and Panorama files.")
        st.stop()

    tmp_paths = []
    prev_master_path = None
    out_path = None

    try:
        if prev_master_file is not None:
            prev_master_path = save_uploaded_file(prev_master_file)
            tmp_paths.append(prev_master_path)

        ally_path = save_uploaded_file(ally_file)
        pan_path = save_uploaded_file(pan_file)
        tmp_paths.extend([ally_path, pan_path])

        out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        out_path = Path(out_tmp.name)
        out_tmp.close()

        with st.spinner("Building monthly master report... (large files can take a bit)"):
            build_monthly_master_report(
                prev_master=prev_master_path,
                ally_current=ally_path,
                pan_current=pan_path,
                output_path=out_path,
                report_month=report_month.strip(),
                term_filter=term_filter.strip() or None,
                dept_ids_filter=dept_ids_filter,
                pan_sheet_override=pan_sheet.strip() or None,
                keep_only_prev_courses=keep_only_prev and (prev_master_path is not None),
                reset_to_year=reset_year.strip() or None,
                exclude_zero_enrollment=exclude_zero_enrollment,
            )

        with open(out_path, "rb") as f:
            st.success("Master report generated successfully.")
            fname = f"accessibility_master_{report_month.strip()}.xlsx"
            st.download_button(
                label="Download Master Excel",
                data=f,
                file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    except Exception as e:
        st.exception(e)

    finally:
        for p in tmp_paths:
            p.unlink(missing_ok=True)
        if out_path is not None:
            try:
                os.unlink(out_path)
            except Exception:
                pass



