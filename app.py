# ================================
# SARMAAN II UPDATED QC DASHBOARD (OPTIMIZED + FORCE REFRESH + DYNAMIC KPIs)
# ================================

from datetime import date
import pandas as pd
import numpy as np
import streamlit as st
import requests
from io import BytesIO

# ---------------- CONFIG ----------------
st.set_page_config(
    page_title="SARMAAN II QC Dashboard (Responsive)",
    layout="wide"
)

# ---------------- DATA SOURCE ----------------
DATA_URL = "https://kf.kobotoolbox.org/api/v2/assets/aLJKVSdGWdGybZznuKXFCM/export-settings/esyo5XY29VoLgyLsGRg4mNz/data.xlsx"
MAIN_SHEET = "SARMAAN II Mortality form- D..."
FEMALES_SHEET = "females"
PREG_SHEET = "pregnancy_history"

# ---------------- SAFE DATA LOADER ----------------
@st.cache_data(show_spinner=True)
def load_data():
    try:
        response = requests.get(DATA_URL, timeout=60)
        response.raise_for_status()
        excel_file = BytesIO(response.content)
        data_dict = pd.read_excel(excel_file, sheet_name=None)

        df_mortality = data_dict[MAIN_SHEET]
        df_females = data_dict[FEMALES_SHEET]
        df_preg = data_dict[PREG_PEG := PREG_SHEET] if False else data_dict[PREG_SHEET]  # keep variable as PREG_SHEET; no change

        # Ensure proper datetime for filtering
        if "start" in df_mortality.columns:
            df_mortality["start"] = pd.to_datetime(df_mortality["start"], errors='coerce')

        return df_mortality, df_females, df_preg
    except Exception as e:
        st.error(f"Error loading workbook: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# ---------------- HELPER ----------------
def find_column_with_suffix(df, keyword):
    """Return first column name containing keyword (case-insensitive), or None."""
    if df is None:
        return None
    for col in df.columns:
        if keyword.lower() in col.lower():
            return col
    return None

# ---------------- QC ENGINE ----------------
def generate_qc_dataframe(df_mortality, df_females, df_preg_history):
    # Find important columns (may return None if not found)
    outcome_col = find_column_with_suffix(df_preg_history, "Was the baby born alive")
    still_alive_col = find_column_with_suffix(df_preg_history, "still alive")  # Q124
    boys_dead_col = find_column_with_suffix(df_females, "boys have died")
    girls_dead_col = find_column_with_suffix(df_females, "daughters have died")
    c_alive_col = find_column_with_suffix(df_females, "c_alive")
    c_dead_col = find_column_with_suffix(df_females, "c_dead")
    miscarriage_col = find_column_with_suffix(df_females, "misscarraige")

    # Defensive: if some female-level cols are missing, create dummy numeric columns to avoid crashes
    for col in [c_alive_col, c_dead_col, miscarriage_col, boys_dead_col, girls_dead_col]:
        if col not in df_females.columns:
            df_females[col] = 0
            # update name variable to point to an existing column if None
            if col is None:
                pass

    # Aggregate females per household
    females_agg = df_females.groupby('_submission__uuid').agg({
        c_alive_col: 'sum',
        c_dead_col: 'sum',
        miscarriage_col: 'sum',
        boys_dead_col: 'sum',
        girls_dead_col: 'sum'
    }).reset_index()
    # total children died (boys + girls) — keep, but not used for the new checks (left for compatibility)
    females_agg['total_children_died'] = females_agg[boys_dead_col].fillna(0) + females_agg[girls_dead_col].fillna(0)

    # ---------------- UPDATED PREGNANCY-LEVEL AGGREGATIONS ----------------
    # We'll compute per _submission__uuid:
    # - Born_Alive_count: count of records where outcome_col == "Born Alive" AND still_alive_col == "Yes"
    # - Later_Died: count of still_alive_col == "No"
    # - Miscarriage_Abortion_count: count of outcome in {"Miscarriage and Abortion", "Born dead"}

    # Defensive: if outcome_col or still_alive_col missing, replace with dummy columns in df_preg_history to avoid KeyError
    preg = df_preg_history.copy()
    if outcome_col not in preg.columns:
        preg['_outcome_dummy'] = np.nan
        outcome_col = '_outcome_dummy'
    if still_alive_col not in preg.columns:
        preg['_still_alive_dummy'] = np.nan
        still_alive_col = '_still_alive_dummy'

    def per_submission_agg(g):
        # Born alive AND still alive == Yes
        born_alive_and_alive = ((g[outcome_col] == "Born Alive") & (g[still_alive_col] == "Yes")).sum()

        # Later died: count of still_alive_col == "No"
        later_died = (g[still_alive_col] == "No").sum()

        # Miscarriage count: count of outcome == "Miscarriage and Abortion" OR "Born dead"
        miscarriage_count = ((g[outcome_col] == "Miscarriage and Abortion") | (g[outcome_col] == "Born dead")).sum()

        # Also compute born dead raw if needed (birth outcome == "Born dead")
        born_dead_raw = (g[outcome_col] == "Born dead").sum()

        return pd.Series({
            "Born_Alive": int(born_alive_and_alive),
            "Later_Died": int(later_died),
            "Miscarriage_Abortion": int(miscarriage_count),
            "Born_Dead_Raw": int(born_dead_raw)
        })

    preg_counts = preg.groupby('_submission__uuid').apply(per_submission_agg).reset_index()

    # Merge females_agg with pregnancy aggregates
    merged = females_agg.merge(preg_counts, on="_submission__uuid", how="left").fillna(0)

    # ---------------- QC logic using the new rules ----------------
    qc_rows = []
    for _, row in merged.iterrows():
        errors = []

        # 1) Born Alive: females.c_alive must equal number of Born_Alive (Born Alive & still alive yes)
        if c_alive_col and c_alive_col in row and int(row[c_alive_col]) != int(row['Born_Alive']):
            errors.append("Born Alive mismatch")

        # 2) Miscarriage_count: females.misscarraige_count must equal preg count of (Miscarriage and Abortion + Born dead)
        if miscarriage_col and miscarriage_col in row and int(row[miscarriage_col]) != int(row['Miscarriage_Abortion']):
            errors.append("Miscarrage mismatch")

        # 3) Born Alive but Later Died: females.c_dead must equal number of Q124 == "No" (Later_Died)
        if c_dead_col and c_dead_col in row and int(row[c_dead_col]) != int(row['Later_Died']):
            errors.append("Born Alive but Later Died mismatch")

        # Keep previous check for total_children_died vs Later_Died if you still want it flagged (optional)
        # (I will not add this as it's redundant with the above rules; keep original behavior minimal)

        qc_rows.append({
            "_submission__uuid": row['_submission__uuid'],
            "QC_Issues": "; ".join(errors) if errors else "No Errors",
            "Total_Flags": len(errors)
        })

    qc_df = pd.DataFrame(qc_rows)

    # ---------------- MAP ENUMERATOR ----------------
    ra_col = find_column_with_suffix(df_mortality, "Type in your Name")
    if ra_col and ra_col in df_mortality.columns:
        qc_df = qc_df.merge(
            df_mortality[["_uuid", ra_col]],
            left_on="_submission__uuid",
            right_on="_uuid",
            how="left"
        ).rename(columns={ra_col: "Research_Assistant"})
    else:
        qc_df["Research_Assistant"] = np.nan

    qc_df.drop(columns=["_uuid"], inplace=True, errors='ignore')
    qc_df["Error_Percentage"] = (qc_df["Total_Flags"] / 4) * 100

    return qc_df

# ---------------- MAIN DASHBOARD FUNCTION ----------------
def run_dashboard():
    # Load data
    df_mortality, df_females, df_preg = load_data()

    if df_females.empty or df_mortality.empty or df_preg.empty:
        st.stop()

    # Filters
    LGA_COL = "Confirm your LGA"
    WARD_COL = "Confirm your ward"
    COMMUNITY_COL = "Confirm your community"
    RA_COL = "Type in your Name"
    DATE_COL = "start"

    def apply_filters(df):
        selected_lga = st.sidebar.selectbox("Confirm your LGA", ["All"] + sorted(df[LGA_COL].dropna().unique()))
        if selected_lga != "All":
            df = df[df[LGA_COL] == selected_lga]

        ward_options = ["All"] + sorted(df[WARD_COL].dropna().unique())
        selected_ward = st.sidebar.selectbox("Confirm your ward", ward_options)
        if selected_ward != "All":
            df = df[df[WARD_COL] == selected_ward]

        community_options = ["All"] + sorted(df[COMMUNITY_COL].dropna().unique())
        selected_community = st.sidebar.selectbox("Confirm your community", community_options)
        if selected_community != "All":
            df = df[df[COMMUNITY_COL] == selected_community]

        ra_options = ["All"] + sorted(df[RA_COL].dropna().unique())
        selected_ra = st.sidebar.selectbox("Type in your Name", ra_options)
        if selected_ra != "All":
            df = df[df[RA_COL] == selected_ra]

        unique_dates = ["All"] + sorted(df[DATE_COL].dropna().dt.date.unique())
        selected_date = st.sidebar.selectbox("Data Collection Date", unique_dates)
        if selected_date != "All":
            df = df[df[DATE_COL].dt.date == selected_date]

        return df

    filtered_final = apply_filters(df_mortality)

    # Filter QC
    submission_ids = filtered_final['_uuid'].unique()
    filtered_females = df_females[df_females['_submission__uuid'].isin(submission_ids)]
    filtered_preg = df_preg[df_preg['_submission__uuid'].isin(submission_ids)]

    # QC DF
    df_qc = generate_qc_dataframe(df_mortality, df_females, df_preg)
    filtered_df = df_qc[df_qc['_submission__uuid'].isin(filtered_final['_uuid'])]

    # KPI CARDS
    st.title("📊 SARMAAN II - Updated QC Dashboard")
    cols = st.columns(4)
    cols[0].metric("Total Households Reached", filtered_final['_uuid'].nunique())
    cols[1].metric("Active Enumerators", filtered_final[RA_COL].nunique())
    cols[2].metric("Wards Reached", filtered_final[WARD_COL].nunique())
    cols[3].metric("Communities Reached", filtered_final[COMMUNITY_COL].nunique())
    st.markdown("---")

    # QC CARDS
    st.subheader("🚨 QC Summary")
    cols = st.columns(6)
    cols[0].metric("Duplicate Household", filtered_final.duplicated(subset="unique_code").sum())
    cols[1].metric("Duplicate Mother", filtered_females.duplicated(subset="mother_id").sum())
    cols[2].metric("Duplicate Child", filtered_preg.duplicated(subset="child_id").sum())
    cols[3].metric("Born Alive mismatch", (filtered_df["QC_Issues"].str.contains("Born Alive mismatch")).sum())
    cols[4].metric("Born Dead mismatch", (filtered_df["QC_Issues"].str.contains("Born Alive but Later Died mismatch")).sum())
    cols[5].metric("Miscarrage mismatch", (filtered_df["QC_Issues"].str.contains("Miscarrage mismatch")).sum())
    st.markdown("---")

    # Errors by RA
    st.subheader("📉 QC Errors by Enumerator")
    error_by_ra = filtered_df.groupby("Research_Assistant")['Total_Flags'].sum().reset_index()
    st.bar_chart(error_by_ra.set_index("Research_Assistant"), use_container_width=True)

    # QC Table
    st.subheader("📋 QC Records and Errors per Enumerator")
    st.dataframe(
        filtered_df[[
            "Research_Assistant",
            '_submission__uuid',
            'QC_Issues',
            'Total_Flags',
            'Error_Percentage'
        ]],
        use_container_width=True,
        height=500
    )

    # CSS
    st.markdown(
        """
        <style>
        .stDataFrame, .stTable {
            width: 100% !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.success("✅ QC Dashboard Updated with Filters, KPIs & Error Analytics")


# ---------------- FORCE REFRESH BUTTON ----------------
if 'refresh' not in st.session_state:
    st.session_state.refresh = False

if st.sidebar.button("🔄 Force Refresh Data"):
    st.cache_data.clear()
    st.session_state.refresh = True

if st.session_state.refresh:
    st.session_state.refresh = False
    run_dashboard()  # Force reload
else:
    run_dashboard()
