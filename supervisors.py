# ================================
# SARMAAN II UPDATED QC DASHBOARD (OPTIMIZED + FORCE REFRESH + LIVE DATA)
# ================================

from datetime import date
import pandas as pd
import numpy as np
import streamlit as st
import requests
from io import BytesIO

# ---------------- SESSION STATE INITIALIZATION ----------------
if 'usage_count' not in st.session_state:
    st.session_state.usage_count = 0
if 'refresh' not in st.session_state:
    st.session_state.refresh = False
if 'force_refresh_trigger' not in st.session_state:
    st.session_state.force_refresh_trigger = False

# ---------------- CONFIG ----------------
st.set_page_config(
    page_title="SARMAAN II QC Dashboard CLUSTER 2",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- START: Custom CSS ---
st.markdown(
    """
    <style>
    .big-title { font-size: 2.5em; font-weight: 700; color: #000000; margin-bottom: 0.5em; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem; font-weight: 600; color: #708090; }
    h2 { border-bottom: 2px solid #f0f2f6; padding-bottom: 10px; margin-top: 1.5em; color: #333333; }
    .stSidebar { background-color: #f7f9fc; }
    .stDataFrame, .stTable { width: 100% !important; }
    .st-emotion-cache-p5m854 { padding-top: 0rem; }
    .custom-metric-value { font-size: 2rem; font-weight: 600; margin-top: 0px; }
    .custom-metric-label { font-size: 0.9rem; font-weight: 600; color: #708090; margin-bottom: 0px; }
    .usage-bar-container { padding: 5px 15px; background-color: rgb(232, 245, 233); border-radius: 0.5rem; margin-bottom: 15px; border: 1px solid rgb(76, 175, 80); display: flex; align-items: center; justify-content: space-between; }
    .usage-text { color: rgb(76, 175, 80); font-weight: 600; font-size: 0.9rem; }
    .stSidebar .stSelectbox label { color: #333333; }
    </style>
    """,
    unsafe_allow_html=True
)
# --- END: Custom CSS ---

# ---------------- DATA SOURCE ----------------
DATA_URL = "https://kf.kobotoolbox.org/api/v2/assets/aMaahuu5VANkY6o4QyQ8uC/export-settings/esK39EhRdJ3yz4wXMsKrJiC/data.xlsx"
MAIN_SHEET = "mortality_pilot_cluster_two-..."
FEMALES_SHEET = "female"
PREG_SHEET = "pregnancy_history"

# ---------------- SAFE DATA LOADER ----------------
def load_data(force_refresh=False):
    """
    Load the latest data from the server.
    If force_refresh=True, bypass cache and pull fresh data.
    """
    cache_key = "sarmaan_data"

    if not force_refresh:
        # Try to get cached data
        cached = st.session_state.get(cache_key)
        if cached is not None:
            return cached

    try:
        response = requests.get(DATA_URL, timeout=60)
        response.raise_for_status()
        excel_file = BytesIO(response.content)
        data_dict = pd.read_excel(excel_file, sheet_name=None)

        df_mortality = data_dict.get(MAIN_SHEET, pd.DataFrame())
        df_females = data_dict.get(FEMALES_SHEET, pd.DataFrame())
        df_preg = data_dict.get(PREG_SHEET, pd.DataFrame())

        if "start" in df_mortality.columns:
            df_mortality["start"] = pd.to_datetime(df_mortality["start"], errors='coerce')

        # Cache the fresh data
        st.session_state[cache_key] = (df_mortality, df_females, df_preg)
        return df_mortality, df_females, df_preg

    except Exception as e:
        st.error(f"❌ Error loading workbook: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# ---------------- HELPER FUNCTIONS ----------------
def find_column_with_suffix(df, keyword):
    if df is None:
        return None
    for col in df.columns:
        if keyword.lower() in col.lower():
            return col
    return None

def generate_qc_dataframe(df_mortality, df_females, df_preg_history):
    outcome_col = find_column_with_suffix(df_preg_history, "Was the baby born alive")
    still_alive_col = find_column_with_suffix(df_preg_history, "still alive")
    boys_dead_col = find_column_with_suffix(df_females, "boys have died")
    girls_dead_col = find_column_with_suffix(df_females, "daughters have died")
    c_alive_col = find_column_with_suffix(df_females, "c_alive")
    c_dead_col = find_column_with_suffix(df_females, "c_dead")
    miscarriage_col = find_column_with_suffix(df_females, "misscarraige")

    female_cols = {
        'c_alive_col': c_alive_col,
        'c_dead_col': c_dead_col,
        'miscarriage_col': miscarriage_col,
        'boys_dead_col': boys_dead_col,
        'girls_dead_col': girls_dead_col
    }

    for name, col in female_cols.items():
        if col is None:
            dummy_col = f'_{name}_dummy'
            df_females[dummy_col] = 0
            female_cols[name] = dummy_col
        elif col not in df_females.columns:
            df_females[col] = 0

    c_alive_col, c_dead_col, miscarriage_col, boys_dead_col, girls_dead_col = \
        female_cols['c_alive_col'], female_cols['c_dead_col'], female_cols['miscarriage_col'], \
        female_cols['boys_dead_col'], female_cols['girls_dead_col']

    # --- NEW: Identify Duplicates based on key identifiers ---
    UNIQUE_CODE_COL = find_column_with_suffix(df_mortality, "unique_code") or 'unique_code'
    
    # Identify duplicate submissions (keep=False marks all duplicates as True)
    # The _uuid/submission__uuid is used to link the error back to the main survey submission
    mortality_dupes = df_mortality[df_mortality.duplicated(subset=UNIQUE_CODE_COL, keep=False)]
    females_dupes = df_females[df_females.duplicated(subset="mother_id", keep=False)]
    preg_dupes = df_preg_history[df_preg_history.duplicated(subset="child_id", keep=False)]

    # Get the UUIDs of the submissions that contain a duplicate record
    dupe_household_uuids = mortality_dupes['_uuid'].unique()
    dupe_mother_uuids = females_dupes['_submission__uuid'].unique()
    dupe_child_uuids = preg_dupes['_submission__uuid'].unique()
    # ---------------------------------------------------------
    
    females_agg = df_females.groupby('_submission__uuid').agg({
        c_alive_col: 'sum',
        c_dead_col: 'sum',
        miscarriage_col: 'sum',
        boys_dead_col: 'sum',
        girls_dead_col: 'sum'
    }).reset_index()
    females_agg['total_children_died'] = females_agg[boys_dead_col].fillna(0) + females_agg[girls_dead_col].fillna(0)

    preg = df_preg_history.copy()
    if outcome_col not in preg.columns:
        preg['_outcome_dummy'] = np.nan
        outcome_col = '_outcome_dummy'
    if still_alive_col not in preg.columns:
        preg['_still_alive_dummy'] = np.nan
        still_alive_col = '_still_alive_dummy'

    def per_submission_agg(g):
        born_alive_and_alive = ((g[outcome_col] == "Born Alive") & (g[still_alive_col] == "Yes")).sum()
        later_died = (g[still_alive_col] == "No").sum()
        miscarriage_count = ((g[outcome_col] == "Miscarriage and Abortion") | (g[outcome_col] == "Born dead")).sum()
        born_dead_raw = (g[outcome_col] == "Born dead").sum()
        return pd.Series({
            "Born_Alive": int(born_alive_and_alive),
            "Later_Died": int(later_died),
            "Miscarriage_Abortion": int(miscarriage_count),
            "Born_Dead_Raw": int(born_dead_raw)
        })

    preg_counts = preg.groupby('_submission__uuid').apply(per_submission_agg).reset_index()
    merged = females_agg.merge(preg_counts, on="_submission__uuid", how="left").fillna(0)

    qc_rows = []
    for _, row in merged.iterrows():
        errors = []
        uuid = row['_submission__uuid']
        
        # Internal Consistency Errors
        if c_alive_col and int(row[c_alive_col]) != int(row['Born_Alive']):
            errors.append("Born Alive mismatch")
        if miscarriage_col and int(row[miscarriage_col]) != int(row['Miscarriage_Abortion']):
            errors.append("Miscarrage mismatch")
        if c_dead_col and int(row[c_dead_col]) != int(row['Later_Died']):
            errors.append("Born Alive but Later Died mismatch")
            
        # Duplication Errors (NEW ADDITION)
        if uuid in dupe_household_uuids:
            errors.append("Duplicate Household")
        if uuid in dupe_mother_uuids:
            errors.append("Duplicate Mother")
        if uuid in dupe_child_uuids:
            errors.append("Duplicate Child")
            
        qc_rows.append({
            "_submission__uuid": uuid,
            "QC_Issues": "; ".join(errors) if errors else "No Errors",
            "Total_Flags": len(errors)
        })

    qc_df = pd.DataFrame(qc_rows)

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
    # Updated divisor to 6 (3 internal checks + 3 duplicate checks)
    qc_df["Error_Percentage"] = (qc_df["Total_Flags"] / 6) * 100 
    return qc_df

def display_qc_metric(col_obj, label, value):
    icon = "✅" if value == 0 else "🚫"
    color = "#333333" if value == 0 else "#D32F2F"
    col_obj.markdown(
        f"""
        <p class="custom-metric-label">{icon} {label}</p>
        <h4 class="custom-metric-value" style="color: {color};">{value:,}</h4>
        """,
        unsafe_allow_html=True
    )

# ---------------- RUN DASHBOARD ----------------
def run_dashboard(df_mortality, df_females, df_preg):
    st.session_state.usage_count += 1
    st.markdown(
        f"""
        <div class="usage-bar-container">
            <span class="usage-text">
                Dashboard Usage Count (Runs/Refreshes): <strong>{st.session_state.usage_count}</strong>
            </span>
        </div>
        """,
        unsafe_allow_html=True
    )

    if df_females.empty or df_mortality.empty or df_preg.empty:
        st.stop()

    # ---------------- EXACT COLUMN NAMES ----------------
    LGA_COL = "Confirm your LGA"
    WARD_COL = "Confirm your ward"
    COMMUNITY_COL = "Confirm your community"
    RA_COL = "Type in your Name"
    DATE_COL = "start"
    VALIDATION_COL = "_validation_status"
    UNIQUE_CODE_COL = find_column_with_suffix(df_mortality, "unique_code") or 'unique_code'
    CONSENT_DATE_COL_RAW = find_column_with_suffix(df_mortality, "consent_date") or DATE_COL

    # --- Sidebar Filters ---
    with st.sidebar:
        st.header("Data Filters")
        st.caption(f"LGA: `{LGA_COL}` | RA: `{RA_COL}`")
        st.markdown("---")

        def apply_filters(df):
            selected_lga = st.selectbox("LGA", ["All"] + sorted(df[LGA_COL].dropna().unique()))
            if selected_lga != "All":
                df = df[df[LGA_COL] == selected_lga]

            selected_ward = st.selectbox("Ward", ["All"] + sorted(df[WARD_COL].dropna().unique()))
            if selected_ward != "All":
                df = df[df[WARD_COL] == selected_ward]

            selected_community = st.selectbox("Community", ["All"] + sorted(df[COMMUNITY_COL].dropna().unique()))
            if selected_community != "All":
                df = df[df[COMMUNITY_COL] == selected_community]

            selected_ra = st.selectbox("Research Assistant", ["All"] + sorted(df[RA_COL].dropna().unique()))
            if selected_ra != "All":
                df = df[df[RA_COL] == selected_ra]

            if DATE_COL in df.columns:
                try:
                    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors='coerce')
                    unique_dates = ["All"] + sorted(df[DATE_COL].dropna().dt.date.unique())
                    selected_date = st.selectbox("Collection Date", unique_dates)
                    if selected_date != "All":
                        df = df[df[DATE_COL].dt.date == selected_date]
                except Exception:
                    st.warning("⚠️ Could not parse Collection Date. Date filter disabled.")
            return df

        filtered_final = apply_filters(df_mortality)

    # Fill validation status
    if VALIDATION_COL in filtered_final.columns:
        filtered_final[VALIDATION_COL].fillna("Validation Ongoing", inplace=True)
        filtered_final = filtered_final[filtered_final[VALIDATION_COL] != "Not Approved"]

    submission_ids = filtered_final['_uuid'].unique()
    filtered_females = df_females[df_females['_submission__uuid'].isin(submission_ids)]
    filtered_preg = df_preg[df_preg['_submission__uuid'].isin(submission_ids)]

    # Use the full dataframes for QC calculation, then filter by submission_ids
    df_qc = generate_qc_dataframe(df_mortality, df_females, df_preg)
    filtered_df = df_qc[df_qc['_submission__uuid'].isin(filtered_final['_uuid'])]

    # --- Dashboard Title & Metrics ---
    st.markdown('<div class="big-title">SARMAAN II - QC Dashboard - Cluster2</div>', unsafe_allow_html=True)
    st.caption("Data Quality Control and Monitoring")

    st.subheader("🎯 Operational Metrics")
    with st.container():
        cols = st.columns(4)
        cols[0].metric("Total Households Reached", f"{filtered_final['_uuid'].nunique():,}")
        cols[1].metric("Active Enumerators", filtered_final[RA_COL].nunique())
        cols[2].metric("Wards Reached", filtered_final[WARD_COL].nunique())
        cols[3].metric("Communities Reached", filtered_final[COMMUNITY_COL].nunique())

    # ---------------- QC Summary ----------------
    st.subheader("🚨 Quality Control Summary")
    with st.container():
        cols = st.columns(6)
        
        # Recalculate Duplicates based on the filtered data for metric display
        duplicate_household = filtered_final.duplicated(subset=UNIQUE_CODE_COL).sum()
        # Find mother/child duplicates within the currently filtered set of submissions
        filtered_mother_dupes = filtered_females[filtered_females['_submission__uuid'].isin(filtered_final['_uuid'])]
        filtered_child_dupes = filtered_preg[filtered_preg['_submission__uuid'].isin(filtered_final['_uuid'])]
        duplicate_mother = filtered_mother_dupes.duplicated(subset="mother_id").sum()
        duplicate_child = filtered_child_dupes.duplicated(subset="child_id").sum()
        
        # Count the number of unique submissions flagged with each error type
        born_alive_mismatch = (filtered_df["QC_Issues"].str.contains("Born Alive mismatch")).sum()
        later_died_mismatch = (filtered_df["QC_Issues"].str.contains("Born Alive but Later Died mismatch")).sum()
        miscarriage_mismatch = (filtered_df["QC_Issues"].str.contains("Miscarrage mismatch")).sum()
        
        # Note: The Duplication counts here are based on the number of *submissions* flagged, 
        # which is the most appropriate metric for the QC summary
        display_qc_metric(cols[0], "Duplicate Household", (filtered_df["QC_Issues"].str.contains("Duplicate Household")).sum())
        display_qc_metric(cols[1], "Duplicate Mother", (filtered_df["QC_Issues"].str.contains("Duplicate Mother")).sum())
        display_qc_metric(cols[2], "Duplicate Child", (filtered_df["QC_Issues"].str.contains("Duplicate Child")).sum())
        display_qc_metric(cols[3], "Born Alive Mismatch", born_alive_mismatch)
        display_qc_metric(cols[4], "B.Alive, Later Died Mismatch", later_died_mismatch)
        display_qc_metric(cols[5], "Miscarriage Mismatch", miscarriage_mismatch)

    # ---------------- Errors by Enumerator ----------------
    st.subheader("📈 QC Errors by Enumerator")
    error_by_ra = filtered_df.groupby("Research_Assistant")['Total_Flags'].sum().reset_index()
    error_by_ra = error_by_ra.sort_values(by='Total_Flags', ascending=False)
    st.bar_chart(
        error_by_ra.set_index("Research_Assistant"),
        use_container_width=True,
        color="#D32F2F"
    )

    # ---------------- Detailed Error Records ----------------
    st.subheader("📋 Detailed Error Records")
    display_df = filtered_df.copy()
    
    # --- CRITICAL CHANGE: FILTER TO ONLY SHOW RECORDS WITH ERRORS ---
    display_df = display_df[display_df['Total_Flags'] > 0]
    # ---------------------------------------------------------------

    dupe_cols = ['_uuid', UNIQUE_CODE_COL, CONSENT_DATE_COL_RAW, VALIDATION_COL, LGA_COL, WARD_COL, COMMUNITY_COL]
    present_dupe_cols = [col for col in dupe_cols if col in filtered_final.columns]
    dupe_df = filtered_final[present_dupe_cols].rename(columns={'_uuid': '_submission__uuid'})
    if CONSENT_DATE_COL_RAW in dupe_df.columns and pd.api.types.is_datetime64_any_dtype(dupe_df[CONSENT_DATE_COL_RAW]):
        dupe_df[CONSENT_DATE_COL_RAW] = dupe_df[CONSENT_DATE_COL_RAW].dt.strftime('%Y-%m-%d')
    display_df = display_df.merge(dupe_df, on="_submission__uuid", how="left")

    display_df.rename(columns={
        LGA_COL: 'LGA',
        WARD_COL: 'Ward',
        COMMUNITY_COL: 'Community',
        'Total_Flags': 'Total Flags',
        'Error_Percentage': 'Error %',
        '_submission__uuid': 'Submission UUID',
        UNIQUE_CODE_COL: 'Unique Code',
        CONSENT_DATE_COL_RAW: 'Date of Consent',
        VALIDATION_COL: 'Validation Status'
    }, inplace=True)
    
    # Select and order columns for display
    display_cols = [
        'Submission UUID', 'Unique Code', 'Research_Assistant', 'Total Flags', 
        'Error %', 'QC_Issues', 'LGA', 'Ward', 'Community', 'Date of Consent', 
        'Validation Status'
    ]
    
    # Ensure only existing columns are passed to the dataframe display
    display_cols = [col for col in display_cols if col in display_df.columns]
    
    st.dataframe(display_df[display_cols], use_container_width=True, height=500)
    st.success("✅ QC Dashboard Updated. Duplication checks included and table filtered to show errors only.")

# ---------------- FORCE REFRESH BUTTON ----------------
with st.sidebar:
    st.markdown("---")
    if st.button("🔄 Force Refresh Data"):
        st.session_state.refresh = True
        st.session_state.force_refresh_trigger = not st.session_state.force_refresh_trigger

# ---------------- INITIAL LOAD ----------------
force_refresh_flag = st.session_state.get('refresh', False) or st.session_state.get('force_refresh_trigger', False)
df_mortality, df_females, df_preg = load_data(force_refresh=force_refresh_flag)
st.session_state.refresh = False
run_dashboard(df_mortality, df_females, df_preg)

