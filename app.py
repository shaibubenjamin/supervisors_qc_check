# ================================
# SARMAAN II UPDATED QC DASHBOARD (OPTIMIZED + FORCE REFRESH + DYNAMIC KPIs)
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

# ---------------- CONFIG ----------------
st.set_page_config(
    page_title="SARMAAN II QC Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- START: Custom CSS for light mode aesthetics and cleaner look ---
st.markdown(
    """
    <style>
    /* Ensure Light Mode dominance and clean aesthetic */
    
    /* 1. Dashboard Title Styling (Changed to Pure Black) */
    .big-title {
        font-size: 2.5em;
        font-weight: 700;
        color: #000000; /* Pure Black as requested */
        margin-bottom: 0.5em;
    }

    /* 2. Streamlit Metric Enhancements */
    /* stMetricValue is styled by the custom metric function for consistency */
    [data-testid="stMetricLabel"] {
        font-size: 0.9rem;
        font-weight: 600;
        color: #708090; /* Slate Gray for labels */
    }

    /* 3. Subheader Styling for distinction (ALL H2 ARE DARK: #333333) */
    h2 {
        border-bottom: 2px solid #f0f2f6; /* Subtle separator */
        padding-bottom: 10px;
        margin-top: 1.5em;
        color: #333333; /* Dark text for light background */
    }
    
    /* 4. Sidebar enhancements */
    .stSidebar {
        background-color: #f7f9fc; /* Very light background for sidebar */
    }
    
    /* 5. Dataframe conditional formatting visibility (re-apply table full width) */
    .stDataFrame, .stTable {
        width: 100% !important;
    }
    
    /* 6. Remove excess spacing around columns/containers */
    .st-emotion-cache-p5m854 { /* Target the main block container */
        padding-top: 0rem;
    }
    
    /* Style for the custom metric boxes to keep consistency */
    .custom-metric-value {
        font-size: 2rem; 
        font-weight: 600;
        margin-top: 0px;
    }
    .custom-metric-label {
        font-size: 0.9rem;
        font-weight: 600;
        color: #708090;
        margin-bottom: 0px;
    }
    
    /* 7. Usage Counter Bar Styling (Matching st.success styling - Green) */
    .usage-bar-container {
        padding: 5px 15px;
        /* Using standard success background and border colors for matching shade */
        background-color: rgb(232, 245, 233); /* Same as st.success background */
        border-radius: 0.5rem;
        margin-bottom: 15px;
        border: 1px solid rgb(76, 175, 80); /* Same as st.success border/icon color */
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .usage-text {
        color: rgb(76, 175, 80); /* Same as st.success text color */
        font-weight: 600;
        font-size: 0.9rem;
    }
    
    /* 8. Target and remove specific elements from Streamlit's rendering */
    .stSidebar .stSelectbox label {
        color: #333333; /* Ensure filter labels are dark */
    }
    </style>
    """,
    unsafe_allow_html=True
)
# --- END: Custom CSS ---


# ---------------- DATA SOURCE ----------------
DATA_URL = "https://kf.kobotoolbox.org/api/v2/assets/abHEibtwS6VnYHZHgupcLR/export-settings/esPDZVAGMh9hjtjYVDfVCiF/data.xlsx"
MAIN_SHEET = "mortality_pilot_cluster_one-..."
FEMALES_SHEET = "female"
PREG_SHEET = "pregnancy_history"

# ---------------- SAFE DATA LOADER ----------------
@st.cache_data(show_spinner="⏳ Loading and processing data... Please wait.", ttl=3600)
def load_data():
    try:
        response = requests.get(DATA_URL, timeout=60)
        response.raise_for_status()
        excel_file = BytesIO(response.content)
        data_dict = pd.read_excel(excel_file, sheet_name=None)

        df_mortality = data_dict[MAIN_SHEET]
        df_females = data_dict[FEMALES_SHEET]
        df_preg = data_dict[PREG_SHEET] 

        # Ensure proper datetime for filtering
        if "start" in df_mortality.columns:
            df_mortality["start"] = pd.to_datetime(df_mortality["start"], errors='coerce')

        return df_mortality, df_females, df_preg
    except Exception as e:
        st.error(f"❌ Error loading workbook: {e}") 
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

# ---------------- QC ENGINE (Functionality unchanged) ----------------
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
    females_agg['total_children_died'] = females_agg[boys_dead_col].fillna(0) + females_agg[girls_dead_col].fillna(0)

    # Pregnancy-level aggregation
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

    # QC logic
    qc_rows = []
    for _, row in merged.iterrows():
        errors = []
        if c_alive_col and c_alive_col in row and int(row[c_alive_col]) != int(row['Born_Alive']):
            errors.append("Born Alive mismatch")
        if miscarriage_col and miscarriage_col in row and int(row[miscarriage_col]) != int(row['Miscarriage_Abortion']):
            errors.append("Miscarrage mismatch")
        if c_dead_col and c_dead_col in row and int(row[c_dead_col]) != int(row['Later_Died']):
            errors.append("Born Alive but Later Died mismatch")
        qc_rows.append({
            "_submission__uuid": row['_submission__uuid'],
            "QC_Issues": "; ".join(errors) if errors else "No Errors",
            "Total_Flags": len(errors)
        })

    qc_df = pd.DataFrame(qc_rows)

    # Map enumerator
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

# Helper function for consistent QC Metric display (per user request)
def display_qc_metric(col_obj, label, value):
    """Displays a QC metric with '✅' if value = 0, or '🚫' and red text if value > 0."""
    icon = "✅" if value == 0 else "🚫"
    color = "#333333" if value == 0 else "#D32F2F" # Dark text for good, Red for error
    
    # Use HTML to display the icon, label, and value clearly
    col_obj.markdown(
        f"""
        <p class="custom-metric-label">
            {icon} {label}
        </p>
        <h4 class="custom-metric-value" style="color: {color};">{value:,}</h4>
        """, 
        unsafe_allow_html=True
    )

def run_dashboard():
    # Increment usage count
    st.session_state.usage_count += 1
    
    # --- USAGE COUNTER BAR (Green) ---
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
    # --------------------------------

    df_mortality, df_females, df_preg = load_data()
    if df_females.empty or df_mortality.empty or df_preg.empty:
        st.stop()

    # Filters and Columns
    LGA_COL = "Confirm your LGA"
    WARD_COL = "Confirm your ward"
    COMMUNITY_COL = "Confirm your community"
    RA_COL = "Type in your Name"
    DATE_COL = "start"
    VALIDATION_COL = "_validation_status" # Column for the new filter/display

    # --- Identify the Consent Date and Unique Code column for merging ---
    CONSENT_DATE_COL_RAW = find_column_with_suffix(df_mortality, "consent_date")
    if not CONSENT_DATE_COL_RAW:
        CONSENT_DATE_COL_RAW = DATE_COL 
    
    UNIQUE_CODE_COL = find_column_with_suffix(df_mortality, "unique_code") or 'unique_code'
    # --------------------------------------------------------

    # --- Improved Sidebar Layout (UX) ---
    with st.sidebar:
        st.header("Data Filters") 
        st.markdown("---")
        
        # Define filter function (unchanged)
        def apply_filters(df):
            selected_lga = st.selectbox("LGA", ["All"] + sorted(df[LGA_COL].dropna().unique()))
            if selected_lga != "All":
                df = df[df[LGA_COL] == selected_lga]
            ward_options = ["All"] + sorted(df[WARD_COL].dropna().unique())
            selected_ward = st.selectbox("Ward", ward_options)
            if selected_ward != "All":
                df = df[df[WARD_COL] == selected_ward]
            community_options = ["All"] + sorted(df[COMMUNITY_COL].dropna().unique())
            selected_community = st.selectbox("Community", community_options)
            if selected_community != "All":
                df = df[df[COMMUNITY_COL] == selected_community]
            ra_options = ["All"] + sorted(df[RA_COL].dropna().unique())
            selected_ra = st.selectbox("Research Assistant", ra_options) # Renamed for clarity
            if selected_ra != "All":
                df = df[df[RA_COL] == selected_ra]
            unique_dates = ["All"] + sorted(df[DATE_COL].dropna().dt.date.unique())
            selected_date = st.selectbox("Collection Date", unique_dates) # Renamed for brevity
            if selected_date != "All":
                df = df[df[DATE_COL].dt.date == selected_date]
            return df

        # Apply spatial and RA filters
        filtered_final = apply_filters(df_mortality)
    # --- End Sidebar ---
    
    # --- NEW MODIFICATION: Fill empty validation status and apply filter ---
    if VALIDATION_COL in filtered_final.columns:
        # Fill NaN values (empty entries) with "Validation Ongoing"
        filtered_final[VALIDATION_COL].fillna("Validation Ongoing", inplace=True)
        # Exclude 'Not Approved' entries
        filtered_final = filtered_final[filtered_final[VALIDATION_COL] != "Not Approved"]
    # ----------------------------------------------------------------------


    submission_ids = filtered_final['_uuid'].unique()
    filtered_females = df_females[df_females['_submission__uuid'].isin(submission_ids)]
    filtered_preg = df_preg[df_preg['_submission__uuid'].isin(submission_ids)]
    df_qc = generate_qc_dataframe(df_mortality, df_females, df_preg)
    filtered_df = df_qc[df_qc['_submission__uuid'].isin(filtered_final['_uuid'])]


    # --- Improved Header and Operational Metrics (KPI CARDS) ---
    # The title is styled via the CSS class .big-title, which now uses the pure black color
    st.markdown('<div class="big-title">SARMAAN II - QC Dashboard</div>', unsafe_allow_html=True)
    st.caption("Data Quality Control and Monitoring")
    
    st.subheader("🎯 Operational Metrics")
    
    with st.container(border=True): # Container for visual grouping
        cols = st.columns(4)
        cols[0].metric("Total Households Reached", f"{filtered_final['_uuid'].nunique():,}", help="Unique household submissions.")
        cols[1].metric("Active Enumerators", filtered_final[RA_COL].nunique(), help="Number of RAs who submitted data for the current filter.")
        cols[2].metric("Wards Reached", filtered_final[WARD_COL].nunique(), help="Unique wards covered.")
        cols[3].metric("Communities Reached", filtered_final[COMMUNITY_COL].nunique(), help="Unique communities covered.")
    
    # ----------------------------------------------------

    # ----------------------------------------------------
    st.subheader("🚨 Quality Control Summary")
    
    with st.container(border=True):
        cols = st.columns(6)
        
        # Duplicates (Now using custom display_qc_metric)
        duplicate_household = filtered_final.duplicated(subset=UNIQUE_CODE_COL).sum()
        duplicate_mother = filtered_females.duplicated(subset="mother_id").sum()
        duplicate_child = filtered_preg.duplicated(subset="child_id").sum()
        
        # QC Mismatch Metrics 
        born_alive_mismatch = (filtered_df["QC_Issues"].str.contains("Born Alive mismatch")).sum()
        later_died_mismatch = (filtered_df["QC_Issues"].str.contains("Born Alive but Later Died mismatch")).sum()
        miscarriage_mismatch = (filtered_df["QC_Issues"].str.contains("Miscarrage mismatch")).sum()

        # Apply the consistent metric display to all 6
        display_qc_metric(cols[0], "Duplicate Household", duplicate_household)
        display_qc_metric(cols[1], "Duplicate Mother", duplicate_mother)
        display_qc_metric(cols[2], "Duplicate Child", duplicate_child)
        display_qc_metric(cols[3], "Born Alive Mismatch", born_alive_mismatch)
        display_qc_metric(cols[4], "B.Alive, Later Died Mismatch", later_died_mismatch)
        display_qc_metric(cols[5], "Miscarriage Mismatch", miscarriage_mismatch)
    
    st.markdown("---")
    # ----------------------------------------------------


    st.subheader("📈 QC Errors by Enumerator")
    
    error_by_ra = filtered_df.groupby("Research_Assistant")['Total_Flags'].sum().reset_index()
    error_by_ra = error_by_ra.sort_values(by='Total_Flags', ascending=False)
    
    st.bar_chart(
        error_by_ra.set_index("Research_Assistant"), 
        use_container_width=True, 
        color="#D32F2F" # Error color (kept red for errors/flags)
    )

    st.subheader("📋 Detailed Error Records")
    
    # Create flags for duplicates
    display_df = filtered_df.copy()
    
    # --- MODIFICATION: Merge in unique code, consent date, and validation status ---
    dupe_cols = ['_uuid', UNIQUE_CODE_COL, CONSENT_DATE_COL_RAW, VALIDATION_COL, LGA_COL, WARD_COL, COMMUNITY_COL]
    
    # Filter dupe_cols to ensure only columns present in filtered_final are used
    present_dupe_cols = [col for col in dupe_cols if col in filtered_final.columns]

    dupe_df = filtered_final[present_dupe_cols].rename(columns={'_uuid': '_submission__uuid'})
    
    # Format the date column before merging for cleaner display
    if CONSENT_DATE_COL_RAW in dupe_df.columns and pd.api.types.is_datetime64_any_dtype(dupe_df[CONSENT_DATE_COL_RAW]):
        dupe_df[CONSENT_DATE_COL_RAW] = dupe_df[CONSENT_DATE_COL_RAW].dt.strftime('%Y-%m-%d')
    
    display_df = display_df.merge(dupe_df, on="_submission__uuid", how="left")
    # ----------------------------------------------------------------------------------------
    
    # Recalculate duplicates markers for display (keeping original functionality)
    unique_codes = filtered_final[UNIQUE_CODE_COL].value_counts()
    duplicate_codes = unique_codes[unique_codes > 1].index
    mother_ids = filtered_females["mother_id"].value_counts()
    duplicate_mothers = mother_ids[mother_ids > 1].index
    child_ids = filtered_preg["child_id"].value_counts()
    duplicate_children = child_ids[child_ids > 1].index
    
    display_df['Duplicate_Household'] = display_df['_submission__uuid'].apply(
        lambda x: '🚨' if filtered_final[filtered_final['_uuid'] == x][UNIQUE_CODE_COL].iloc[0] in duplicate_codes else ''
    )
    display_df['Duplicate_Mother'] = display_df['_submission__uuid'].apply(
        lambda x: '🚨' if any(filtered_females[filtered_females['_submission__uuid'] == x]['mother_id'].isin(duplicate_mothers)) else ''
    )
    display_df['Duplicate_Child'] = display_df['_submission__uuid'].apply(
        lambda x: '🚨' if any(filtered_preg[filtered_preg['_submission__uuid'] == x]['child_id'].isin(duplicate_children)) else ''
    )

    # Keep only rows with errors or duplicates
    display_df = display_df[
        (display_df["Total_Flags"] > 0) |
        (display_df['Duplicate_Household'] != '') |
        (display_df['Duplicate_Mother'] != '') |
        (display_df['Duplicate_Child'] != '')
    ].copy()

    # ---------------- CONDITIONAL FORMATTING ----------------
    def highlight_errors(val):
        """Highlights the QC Issues column."""
        if val != "No Errors":
            return 'background-color: #ffe0e6; color: #cc0033; font-weight: bold;'  # Soft pink background, dark red text
        return ''

    def highlight_flag_count(val):
        """Highlights the Total_Flags column."""
        if val > 0:
            return 'background-color: #ffcccc; font-weight: bold;' # Brighter red for the count
        return ''
    
    def highlight_percentage(val):
        """Highlights the Error_Percentage column."""
        if val > 0:
            return 'background-color: #ffcccc; font-weight: bold;'
        return ''
        
    def format_percentage(val):
        """Formats the percentage column."""
        return f'{val:.1f}%' if pd.notna(val) else ''

    # Clean up column names for display
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
    
    # Apply styling
    # --- MODIFICATION: Final columns to display, including new columns ---
    styled_df = display_df[
        ['Research_Assistant', 'Unique Code', 'Validation Status', 'Date of Consent', 'LGA', 'Ward', 'Community', 'Submission UUID', 'QC_Issues', 'Total Flags', 'Error %', 'Duplicate_Household', 'Duplicate_Mother', 'Duplicate_Child']
    ].style \
    .applymap(highlight_errors, subset=['QC_Issues']) \
    .applymap(highlight_flag_count, subset=['Total Flags']) \
    .applymap(highlight_percentage, subset=['Error %']) \
    .format({'Error %': format_percentage})

    st.dataframe(
        styled_df,
        use_container_width=True,
        height=500
    )

    st.success("✅ QC Dashboard Updated. The main title is now in pure black.")


# ---------------- FORCE REFRESH BUTTON (Moved to sidebar for UX) ----------------
with st.sidebar:
    st.markdown("---")
    if st.button("🔄 Force Refresh Data", help="Clears cache and reloads all data from KoBo Toolbox."):
        st.cache_data.clear()
        st.session_state.refresh = True

if st.session_state.refresh:
    st.session_state.refresh = False
    st.rerun() 
else:
    run_dashboard()
