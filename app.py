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
    page_title="SARMAAN II QC Dashboard CLUSTER 1",
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
DATA_URL = "https://kf.kobotoolbox.org/api/v2/assets/abHEibtwS6VnYHZHgupcLR/export-settings/esm7VCuQFJLhymWZPrNhDtg/data.xlsx"
MAIN_SHEET = "mortality_pilot_cluster_one-..."
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
    UNIQUE_CODE_COL = find_column_with_suffix(df_mortality, "unique") or 'unique_code'
    
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

    # ---------------- DYNAMIC COLUMN MAPPING ----------------
    LGA_COL = find_column_with_suffix(df_mortality, "lga") or "Confirm your LGA"
    WARD_COL = find_column_with_suffix(df_mortality, "ward") or "Confirm your ward"
    COMMUNITY_COL = find_column_with_suffix(df_mortality, "community") or "Confirm your community"
    RA_COL = find_column_with_suffix(df_mortality, "name") or "Type in your Name"
    DATE_COL = "start"
    VALIDATION_COL = "_validation_status"
    # Use 'unique' as the keyword to find the Household Unique Code column
    UNIQUE_CODE_COL = find_column_with_suffix(df_mortality, "unique") or 'unique_code' 
    CONSENT_DATE_COL_RAW = find_column_with_suffix(df_mortality, "consent_date") or DATE_COL
    
    # ------------------ NEW COLUMN DISPLAY NAMES ---------------------
    # Map the internal column name (if found) to the requested display name
    COMMUNITY_DISPLAY_NAME = "Confirm your community"
    LGA_DISPLAY_NAME = "LGA" 
    WARD_DISPLAY_NAME = "Ward" 
    RA_DISPLAY_NAME = "Enumerator Name"
    
    # Safely rename Community column for display in metric and filter section
    community_col_for_filter = COMMUNITY_COL if COMMUNITY_COL in df_mortality.columns else None
    # -------------------------------------------------------------------

    # --- Sidebar Filters ---
    with st.sidebar:
        st.header("Data Filters")
        st.caption(f"LGA: `{LGA_COL}` | RA: `{RA_COL}` | Unique ID: `{UNIQUE_CODE_COL}`")
        st.markdown("---")

        # Safely determine if columns exist for filtering
        lga_filter_ok = LGA_COL in df_mortality.columns
        ward_filter_ok = WARD_COL in df_mortality.columns
        community_filter_ok = COMMUNITY_COL in df_mortality.columns
        ra_filter_ok = RA_COL in df_mortality.columns
        
        def apply_filters(df):
            # LGA Filter
            if not lga_filter_ok:
                st.warning(f"⚠️ **LGA Filter Disabled:** Column '{LGA_COL}' not found.")
            else:
                selected_lga = st.selectbox("LGA", ["All"] + sorted(df[LGA_COL].dropna().unique()))
                if selected_lga != "All":
                    df = df[df[LGA_COL] == selected_lga]

            # Ward Filter
            if not ward_filter_ok:
                st.warning(f"⚠️ **Ward Filter Disabled:** Column '{WARD_COL}' not found.")
            else:
                selected_ward = st.selectbox("Ward", ["All"] + sorted(df[WARD_COL].dropna().unique()))
                if selected_ward != "All":
                    df = df[df[WARD_COL] == selected_ward]
            
            # Community Filter - Use COMMUNITY_COL for filtering, but ensure filter label is clear
            if not community_filter_ok:
                st.warning(f"⚠️ **Community Filter Disabled:** Column '{COMMUNITY_COL}' not found.")
            else:
                selected_community = st.selectbox(COMMUNITY_DISPLAY_NAME, ["All"] + sorted(df[COMMUNITY_COL].dropna().unique()))
                if selected_community != "All":
                    df = df[df[COMMUNITY_COL] == selected_community]

            # RA Filter
            if not ra_filter_ok:
                st.warning(f"⚠️ **RA Filter Disabled:** Column '{RA_COL}' not found.")
            else:
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

        # Store the original unfiltered data for "Not Approved" table later
        df_mortality_original = df_mortality.copy()
        
        # Filter the main dataframe used for metrics/QC
        filtered_final = apply_filters(df_mortality)
        
        # The main logic for the rest of the dashboard should *exclude* "Not Approved" records
        if VALIDATION_COL in filtered_final.columns:
            # First, filter out 'Not Approved' from the main dashboard data view
            df_for_metrics = filtered_final[filtered_final[VALIDATION_COL] != "Not Approved"].copy()
            # Then, fill NaN statuses for the metric view only
            df_for_metrics[VALIDATION_COL].fillna("Validation Ongoing", inplace=True)
        else:
            df_for_metrics = filtered_final.copy()
            st.warning("⚠️ Validation Status column not found. All records included in metrics.")


    submission_ids = df_for_metrics['_uuid'].unique()
    filtered_females = df_females[df_females['_submission__uuid'].isin(submission_ids)]
    filtered_preg = df_preg[df_preg['_submission__uuid'].isin(submission_ids)]

    # Use the full dataframes for QC calculation, then filter by submission_ids
    df_qc = generate_qc_dataframe(df_mortality, df_females, df_preg)
    filtered_df = df_qc[df_qc['_submission__uuid'].isin(df_for_metrics['_uuid'])]

    # --- Dashboard Title & Metrics ---
    st.markdown('<div class="big-title">SARMAAN II - QC Dashboard - Cluster1</div>', unsafe_allow_html=True)
    st.caption("Data Quality Control and Monitoring")

    st.subheader("🎯 Operational Metrics")
    with st.container():
        cols = st.columns(4)
        cols[0].metric("Total Households Reached", f"{df_for_metrics['_uuid'].nunique():,}")
        
        # Safely access column for metrics
        ra_col_for_metric = RA_COL if RA_COL in df_for_metrics.columns else None
        ward_col_for_metric = WARD_COL if WARD_COL in df_for_metrics.columns else None
        community_col_for_metric = COMMUNITY_COL if COMMUNITY_COL in df_for_metrics.columns else None

        if ra_col_for_metric:
            cols[1].metric("Active Enumerators", df_for_metrics[ra_col_for_metric].nunique())
        else:
            cols[1].metric("Active Enumerators", "N/A")

        cols[2].metric("Wards Reached", df_for_metrics[ward_col_for_metric].nunique() if ward_col_for_metric else 0)
        
        # Use the specific column if it exists, otherwise count the unique values in the raw column and display the generic name
        if community_col_for_metric:
             cols[3].metric("Communities Reached", df_for_metrics[community_col_for_metric].nunique())
        else:
             cols[3].metric("Communities Reached", 0) # Should be covered by the filter check


    # ---------------- QC Summary ----------------
    st.subheader("🚨 Quality Control Summary (Metrics exclude 'Not Approved')")
    with st.container():
        cols = st.columns(6)
        
        # Count the number of unique submissions flagged with each error type
        born_alive_mismatch = (filtered_df["QC_Issues"].str.contains("Born Alive mismatch")).sum()
        later_died_mismatch = (filtered_df["QC_Issues"].str.contains("Born Alive but Later Died mismatch")).sum()
        miscarriage_mismatch = (filtered_df["QC_Issues"].str.contains("Miscarrage mismatch")).sum()
        
        # Note: The Duplication counts here are based on the number of *submissions* flagged
        display_qc_metric(cols[0], "Duplicate Household", (filtered_df["QC_Issues"].str.contains("Duplicate Household")).sum())
        display_qc_metric(cols[1], "Duplicate Mother", (filtered_df["QC_Issues"].str.contains("Duplicate Mother")).sum())
        display_qc_metric(cols[2], "Duplicate Child", (filtered_df["QC_Issues"].str.contains("Duplicate Child")).sum())
        display_qc_metric(cols[3], "Born Alive Mismatch", born_alive_mismatch)
        display_qc_metric(cols[4], "B.Alive, Later Died Mismatch", later_died_mismatch)
        display_qc_metric(cols[5], "Miscarriage Mismatch", miscarriage_mismatch)

    # ---------------- Not Approved Table (UPDATED: Community Display Name) ----------------
    st.markdown("---")
    st.subheader("❌ Submissions **NOT APPROVED** (Action: Recollection Required)")
    
    if VALIDATION_COL in df_mortality_original.columns:
        not_approved_df = df_mortality_original[df_mortality_original[VALIDATION_COL] == "Not Approved"].copy()

        if not not_approved_df.empty:
            # Set the action column to the literal status "Not Approved"
            not_approved_df['Validation Status'] = "Not Approved"
            
            # Use the actual column names in the list
            display_cols = ['Validation Status', UNIQUE_CODE_COL, RA_COL, LGA_COL, WARD_COL, COMMUNITY_COL, DATE_COL]
            
            # Filter the columns present in the DataFrame and rename
            display_cols = [col for col in display_cols if col in not_approved_df.columns]
            
            display_na_df = not_approved_df[display_cols].rename(columns={
                UNIQUE_CODE_COL: 'Household Unique Code',
                RA_COL: RA_DISPLAY_NAME,
                LGA_COL: LGA_DISPLAY_NAME,
                WARD_COL: WARD_DISPLAY_NAME,
                # CRITICAL CHANGE: Rename the raw column to the requested display name
                COMMUNITY_COL: COMMUNITY_DISPLAY_NAME, 
                DATE_COL: 'Submission Date'
            })
            
            # Format date column
            if 'Submission Date' in display_na_df.columns:
                 display_na_df['Submission Date'] = pd.to_datetime(
                     display_na_df['Submission Date'], errors='coerce'
                 ).dt.strftime('%Y-%m-%d %H:%M')
            
            st.dataframe(display_na_df, use_container_width=True, height=300)
            st.error(f"🔴 **Total Not Approved:** {len(display_na_df):,} submissions must be revisited/recollected.")

        else:
            st.info("✅ No 'Not Approved' submissions found.")
    else:
        st.error(f"❌ Cannot display 'Not Approved' table. Validation Status column ('{VALIDATION_COL}') not found.")

    # ---------------- Errors by Enumerator ----------------
    st.markdown("---")
    st.subheader("📈 QC Errors by Enumerator (Excluding 'Not Approved')")
    error_by_ra = filtered_df.groupby("Research_Assistant")['Total_Flags'].sum().reset_index()
    error_by_ra = error_by_ra.sort_values(by='Total_Flags', ascending=False)
    st.bar_chart(
        error_by_ra.set_index("Research_Assistant"),
        use_container_width=True,
        color="#D32F2F"
    )

    # ---------------- DUPLICATE HOUSEHOLD RECORDS (UPDATED: Community Display Name) ----------------
    st.subheader("🏠 Duplicate Household Submissions (Excluding 'Not Approved')")
    
    if UNIQUE_CODE_COL in df_for_metrics.columns:
        # 1. Identify rows that are duplicates based on the Unique Code in the filtered (non-Not Approved) data
        dupe_mask = df_for_metrics.duplicated(subset=UNIQUE_CODE_COL, keep=False)
        duplicate_households = df_for_metrics[dupe_mask].sort_values(by=UNIQUE_CODE_COL).copy()

        if not duplicate_households.empty:
            # Prepare the display columns
            display_dupe_cols = [
                '_uuid', UNIQUE_CODE_COL, RA_COL, LGA_COL, WARD_COL, 
                COMMUNITY_COL, DATE_COL
            ]
            
            # Filter the columns present in the DataFrame
            display_dupe_cols = [col for col in display_dupe_cols if col in duplicate_households.columns]
            
            # Rename columns for clarity
            display_dupe_df = duplicate_households[display_dupe_cols].rename(columns={
                '_uuid': 'Submission UUID',
                UNIQUE_CODE_COL: 'Household Unique Code (DUPLICATE)',
                RA_COL: RA_DISPLAY_NAME,
                LGA_COL: LGA_DISPLAY_NAME,
                WARD_COL: WARD_DISPLAY_NAME,
                # CRITICAL CHANGE: Rename the raw column to the requested display name
                COMMUNITY_COL: COMMUNITY_DISPLAY_NAME,
                DATE_COL: 'Submission Date',
            })
            
            # Format date column
            if 'Submission Date' in display_dupe_df.columns:
                 display_dupe_df['Submission Date'] = pd.to_datetime(
                     display_dupe_df['Submission Date'], errors='coerce'
                 ).dt.strftime('%Y-%m-%d %H:%M')

            st.dataframe(display_dupe_df, use_container_width=True, height=300)
            st.warning(f"❗ **{len(display_dupe_df):,}** submissions share the same **Unique Code**. They should be reviewed.")
        else:
            st.info("✅ No duplicate household submissions found in the current filter selection.")
    else:
        st.error(f"❌ Cannot check for household duplicates. Unique Code column ('{UNIQUE_CODE_COL}') not found.")


    # ---------------- Detailed Error Records (UPDATED: Community Display Name) ----------------
    st.markdown("---")
    st.subheader("📋 Detailed Internal/Cross-Check Error Records (Excluding 'Not Approved')")
    display_df = filtered_df.copy()
    
    # --- Filter to only show records with errors ---
    display_df = display_df[display_df['Total_Flags'] > 0]
    # -----------------------------------------------

    dupe_cols = ['_uuid', UNIQUE_CODE_COL, CONSENT_DATE_COL_RAW, VALIDATION_COL, LGA_COL, WARD_COL, COMMUNITY_COL, RA_COL]
    present_dupe_cols = [col for col in dupe_cols if col in df_for_metrics.columns]
    dupe_df = df_for_metrics[present_dupe_cols].rename(columns={'_uuid': '_submission__uuid'}).copy()
    
    # Rename RA column in dupe_df to prevent merge confusion if RA_COL was found successfully
    if RA_COL in dupe_df.columns:
        dupe_df.rename(columns={RA_COL: 'Research_Assistant_Merge'}, inplace=True)

    if CONSENT_DATE_COL_RAW in dupe_df.columns and pd.api.types.is_datetime64_any_dtype(dupe_df[CONSENT_DATE_COL_RAW]):
        dupe_df[CONSENT_DATE_COL_RAW] = dupe_df[CONSENT_DATE_COL_RAW].dt.strftime('%Y-%m-%d')
        
    display_df = display_df.merge(dupe_df, on="_submission__uuid", how="left")
    
    # Drop the merged Research_Assistant column as we already have 'Research_Assistant' from the QC calculation
    display_df.drop(columns=["Research_Assistant_Merge"], inplace=True, errors='ignore')


    display_df.rename(columns={
        LGA_COL: LGA_DISPLAY_NAME,
        WARD_COL: WARD_DISPLAY_NAME,
        # CRITICAL CHANGE: Rename the raw column to the requested display name
        COMMUNITY_COL: COMMUNITY_DISPLAY_NAME,
        'Total_Flags': 'Total Flags',
        'Error_Percentage': 'Error %',
        '_submission__uuid': 'Submission UUID',
        UNIQUE_CODE_COL: 'Household Unique Code', 
        CONSENT_DATE_COL_RAW: 'Date of Consent',
        VALIDATION_COL: 'Validation Status',
        # Rename the Research_Assistant column that comes from the QC calculation to the desired display name
        'Research_Assistant': RA_DISPLAY_NAME 
    }, inplace=True)
    
    # Select and order columns for display - Confirmed Community Display Name is included
    display_cols = [
        'Submission UUID', 'Household Unique Code', RA_DISPLAY_NAME, 'Total Flags', 
        'Error %', 'QC_Issues', LGA_DISPLAY_NAME, WARD_DISPLAY_NAME, COMMUNITY_DISPLAY_NAME, 'Date of Consent', 
        'Validation Status'
    ]
    
    # Ensure only existing columns are passed to the dataframe display
    display_cols = [col for col in display_cols if col in display_df.columns]
    
    st.dataframe(display_df[display_cols], use_container_width=True, height=500)
    st.success("✅ Dashboard updated. The 'Community' column is now displayed as 'Confirm your community' in all tables.")

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
