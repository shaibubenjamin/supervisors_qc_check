# ================================
# SARMAAN II UPDATED QC DASHBOARD (OPTIMIZED + FORCE REFRESH + LIVE DATA)
# ================================

from datetime import date
import pandas as pd
import numpy as np
import streamlit as st
import requests
# IMPORTANT: Use io.BytesIO for web download and io.StringIO for string data parsing
from io import BytesIO, StringIO 

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
# Use the URL for Cluster 1 data
DATA_URL = "https://kf.kobotoolbox.org/api/v2/assets/abHEibtwS6VnYHZHgupcLR/export-settings/esm7VCuQFJLhymWZPrNhDtg/data.xlsx"
MAIN_SHEET = "mortality_pilot_cluster_one-..."
FEMALES_SHEET = "female"
PREG_SHEET = "pregnancy_history"

# --- SOP Lookup Table (Cleaned for robust parsing) ---
# NOTE: The data below has been formatted using consistent single tabs (\t) 
# to resolve the previous ParserError.
SOP_DATA = """
lga_Label	ward_Label	settlement_Label	Community_ID
Potiskum	Bare_Bari	Kandahar	B-11_14_1_1
Potiskum	Bare_Bari	Unguwan_Kuka	B-11_14_1_2
Potiskum	Bare_Bari	Jigawa_Chadi	B-11_14_1_3
Potiskum	Bare_Bari	Gadama	B-11_14_1_4
Potiskum	Bare_Bari	Ung_Gada	B-11_14_1_5
Potiskum	Bare_Bari	Jigawa_City_Petroleum	B-11_14_1_6
Potiskum	Bare_Bari	Ung_Kuka	B-11_14_1_7
Potiskum	Bare_Bari	Jigawa_Makabarta	B-11_14_1_8
Potiskum	Bare_Bari	Mangorori	B-11_14_1_9
Potiskum	Bare_Bari	Lai_Lai_Madabi	B-11_14_1_10
Potiskum	Bolewa_A	Madu_K_O	B-11_14_2_1
Potiskum	Bolewa_A	Maiung_Galadima	B-11_14_2_2
Potiskum	Bolewa_A	Abba_Sugu	B-11_14_2_3
Potiskum	Bolewa_A	Mai_Ung_Luccu	B-11_14_2_4
Potiskum	Bolewa_A	Mai_Ung_Bomoi_3	B-11_14_2_5
Potiskum	Bolewa_A	Hakimi_Shuaibu_1	B-11_14_2_6
Potiskum	Bolewa_A	Bomoi_Maina	B-11_14_2_7
Potiskum	Bolewa_A	Chiroma	B-11_14_2_8
Potiskum	Bolewa_A	Lamba_Maaji	B-11_14_2_9
Potiskum	Bolewa_A	Yusuf_Kafinta_1	B-11_14_2_10
Potiskum	Bolewa_B	Muhd_Guza	B-11_14_3_1
Potiskum	Bolewa_B	Alhaji_Ibrahim	B-11_14_3_2
Potiskum	Bolewa_B	Mai_Unguwan_Darin	B-11_14_3_3
Potiskum	Bolewa_B	Baba_Sarki	B-11_14_3_4
Potiskum	Bolewa_B	Mallam_Ali	B-11_14_3_5
Potiskum	Bolewa_B	Mai_Unguwan_Hamidu	B-11_14_3_6
Potiskum	Bolewa_B	Maianguwa_Bukar	B-11_14_3_7
Potiskum	Bolewa_B	Usman_Arjali	B-11_14_3_8
Potiskum	Bolewa_B	New_Secretariat	B-11_14_3_9
Potiskum	Bolewa_B	Layin_Palace	B-11_14_3_10
Potiskum	Danchuwa	Garin_Tori	B-11_14_4_1
Potiskum	Danchuwa	Maina_Bujik	B-11_14_4_2
Potiskum	Danchuwa	Garin_Bah	B-11_14_4_3
Potiskum	Danchuwa	Danchuwa_Lamba	B-11_14_4_4
Potiskum	Danchuwa	Makwai_Bulama_Abdu	B-11_14_4_5
Potiskum	Danchuwa	Bogocho	B-11_14_4_6
Potiskum	Danchuwa	Makwai_Bulama_Yau	B-11_14_4_7
Potiskum	Danchuwa	Babaudu	B-11_14_4_8
Potiskum	Danchuwa	Garin_Bade	B-11_14_4_9
Potiskum	Danchuwa	Sabon_Layi	B-11_14_4_10
Potiskum	Dogo_Nini	Coca_Cola	B-11_14_5_1
Potiskum	Dogo_Nini	Mai_Anguwa_Kagazau	B-11_14_5_2
Potiskum	Dogo_Nini	Lamba_Muhd	B-11_14_5_3
Potiskum	Dogo_Nini	Adamu_Wanzam	B-11_14_5_4
Potiskum	Dogo_Nini	Saidu_Manager	B-11_14_5_5
Potiskum	Dogo_Nini	Lamba_Idrissa	B-11_14_5_6
Gombe	Dogo_Nini	Yan_Shinkafa	B-11_14_5_7
Gombe	Dogo_Nini	Mai_Anguwa_Babayo	B-11_14_5_8
Potiskum	Dogo_Nini	Yan_Gadaje	B-11_14_5_9
Potiskum	Dogo_Nini	Haruna_Dugum	B-11_14_5_10
Potiskum	Dogo_Tebo	Bayan_Cabs	B-11_14_6_1
Potiskum	Dogo_Tebo	Damboa_Area	B-11_14_6_2
Potiskum	Dogo_Tebo	Hassan_Damboa	B-11_14_6_3
Potiskum	Dogo_Tebo	Jujin_Oc	B-11_14_6_4
Potiskum	Dogo_Tebo	Lamba_Goni	B-11_14_6_5
Potiskum	Dogo_Tebo	Hussaini_Damboa	B-11_14_6_6
Potiskum	Dogo_Tebo	Yankuka	B-11_14_6_7
Potiskum	Dogo_Tebo	Ibrahim_Chana	B-11_14_6_8
Potiskum	Dogo_Tebo	Cabs	B-11_14_6_9
Potiskum	Dogo_Tebo	Tinja_Tuya_Street	B-11_14_6_10
Potiskum	Hausawa_Asibiti	Danjebu	B-11_14_7_1
Potiskum	Hausawa_Asibiti	Bayan_Makabarta	B-11_14_7_2
Potiskum	Hausawa_Asibiti	Mai_Madagali	B-11_14_7_3
Potiskum	Hausawa_Asibiti	Rigiyar_Gardi	B-11_14_7_4
Potiskum	Hausawa_Asibiti	Mai_Saleh	B-11_14_7_5
Potiskum	Hausawa_Asibiti	Alhaji_Mato	B-11_14_7_6
Potiskum	Hausawa_Asibiti	Musa_Kuku	B-11_14_7_7
Potiskum	Hausawa_Asibiti	Yaro_Gambo	B-11_14_7_8
Potiskum	Hausawa_Asibiti	Wakili_Audu	B-11_14_7_9
Potiskum	Hausawa_Asibiti	Mai_Usman	B-11_14_7_10
Potiskum	Mamudo	Unguwan_Ali	B-11_14_8_1
Potiskum	Mamudo	Gumbakuku	B-11_14_8_2
Potiskum	Mamudo	Marke_Chayi	B-11_14_8_3
Potiskum	Mamudo	Bubaram_Bilal_Dambam	B-11_14_8_4
Potiskum	Mamudo	Sandawai	B-11_14_8_5
Potiskum	Mamudo	Bula_Hc	B-11_14_8_6
Potiskum	Mamudo	Kama_Kirji	B-11_14_8_7
Potiskum	Mamudo	Zagam	B-11_14_8_8
Potiskum	Mamudo	Adaya_Pri_Sch	B-11_14_8_9
Potiskum	Mamudo	Maina_Buba	B-11_14_8_10
Potiskum	Ngojin_Alaraba	Tokare	B-11_14_9_1
Potiskum	Ngojin_Alaraba	Mbalido	B-11_14_9_2
Potiskum	Ngojin_Alaraba	Hadijam_Gubdo	B-11_14_9_3
Potiskum	Ngojin_Alaraba	Garin_Dala	B-11_14_9_4
Potiskum	Ngojin_Alaraba	Mai_Turare	B-11_14_9_5
Potiskum	Ngojin_Alaraba	Badejo	B-11_14_9_6
Potiskum	Ngojin_Alaraba	Arjali	B-11_14_9_7
Potiskum	Ngojin_Alaraba	Fara_Fara_Bulama	B-11_14_9_8
Potiskum	Ngojin_Alaraba	Mai_Jaarma	B-11_14_9_9
Potiskum	Ngojin_Alaraba	Bulakos	B-11_14_9_10
Potiskum	Yerimaram	Nasarawa_B	B-11_14_10_1
Potiskum	Yerimaram	Yerimaram_Bulama_Lamba_Zubairu	B-11_14_10_2
Potiskum	Yerimaram	Kabono	B-11_14_10_3
Potiskum	Yerimaram	Yawachi	B-11_14_10_4
Potiskum	Yerimaram	Nahuta_Babban_Layi	B-11_14_10_5
Potiskum	Yerimaram	Travellers	B-11_14_10_6
Potiskum	Yerimaram	Hon_Sani	B-11_14_10_7
Potiskum	Yerimaram	Nahuta_Pri_School	B-11_14_10_8
Potiskum	Yerimaram	Mai_Anguwa_Yakubu_33	B-11_14_10_9
Potiskum	Yerimaram	Mai_Anguwa_Sale	B-11_14_10_10
"""

# Load SOP data into a DataFrame
try:
    # Use StringIO to read the string data and ensure sep='\t' and skipinitialspace=True
    SOP_DF = pd.read_csv(StringIO(SOP_DATA), sep='\t', skipinitialspace=True)
    
    # Define expected columns for validation
    EXPECTED_SOP_COLUMNS = ['lga_Label', 'ward_Label', 'settlement_Label', 'Community_ID']
    
    # Validate the column names after loading
    if list(SOP_DF.columns) != EXPECTED_SOP_COLUMNS:
        st.error("❌ SOP Data Loading Error: Column headers were not parsed correctly. Check the tabs/spaces in the header row.")
        SOP_COMMUNITY_MAP = {}
    else:
        # Create a dictionary map from Community_ID (code) to settlement_Label (name)
        SOP_COMMUNITY_MAP = SOP_DF.set_index('Community_ID')['settlement_Label'].to_dict()
except Exception as e:
    st.error(f"❌ Critical SOP Data Parsing Failed: {e}. Please ensure the data rows and headers are separated by a single tab.")
    SOP_COMMUNITY_MAP = {}


# ---------------- SAFE DATA LOADER ----------------
@st.cache_data(show_spinner="Downloading and processing latest KoboToolbox data...", ttl=600)
def load_data(force_refresh=False):
    """
    Load the latest data from the server.
    """
    if force_refresh:
        st.cache_data.clear() # Clear cache on force refresh call

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

        # --- Apply Community Code Mapping (CRITICAL FIX) ---
        COMMUNITY_COL_RAW = find_column_with_suffix(df_mortality, "community")
        
        if COMMUNITY_COL_RAW in df_mortality.columns and SOP_COMMUNITY_MAP:
             # Apply the map. Use the code itself if a mapping is not found (default behavior)
             df_mortality[COMMUNITY_COL_RAW] = df_mortality[COMMUNITY_COL_RAW].astype(str).map(
                 lambda x: SOP_COMMUNITY_MAP.get(x, x)
             )
        
        return df_mortality, df_females, df_preg

    except Exception as e:
        st.error(f"❌ Error loading workbook: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# ---------------- HELPER FUNCTIONS ----------------
def find_column_with_suffix(df, keyword):
    if df is None or df.empty:
        return None
    for col in df.columns:
        if keyword.lower() in col.lower():
            return col
    return None

def generate_qc_dataframe(df_mortality, df_females, df_preg_history):
    # Dynamic column finding
    outcome_col = find_column_with_suffix(df_preg_history, "Was the baby born alive")
    still_alive_col = find_column_with_suffix(df_preg_history, "still alive")
    boys_dead_col = find_column_with_suffix(df_females, "boys have died")
    girls_dead_col = find_column_with_suffix(df_females, "daughters have died")
    c_alive_col = find_column_with_suffix(df_females, "c_alive")
    c_dead_col = find_column_with_suffix(df_females, "c_dead")
    miscarriage_col = find_column_with_suffix(df_females, "misscarraige")
    
    # *** UPDATED: Explicitly look for a column containing "unique_code" ***
    # This assumes the full unique code, like 'B-11_14_2_2_025', lives in a column 
    # named "unique_code" or something similar.
    UNIQUE_CODE_COL = find_column_with_suffix(df_mortality, "unique_code") or 'unique_code_col_not_found' 
    
    # Handle missing columns in sub-tables by creating dummy columns
    female_cols = {
        'c_alive_col': c_alive_col, 'c_dead_col': c_dead_col, 'miscarriage_col': miscarriage_col,
        'boys_dead_col': boys_dead_col, 'girls_dead_col': girls_dead_col
    }
    for name, col in female_cols.items():
        if col is None or col not in df_females.columns:
            dummy_col = f'_{name}_dummy'
            df_females[dummy_col] = 0
            female_cols[name] = dummy_col
    c_alive_col, c_dead_col, miscarriage_col, boys_dead_col, girls_dead_col = female_cols.values()

    # --- Household Duplicate Check ---
    if UNIQUE_CODE_COL in df_mortality.columns:
        mortality_dupes = df_mortality[df_mortality.duplicated(subset=UNIQUE_CODE_COL, keep=False)]
    else:
        # Fallback if unique code column is truly missing, resulting in no duplicates flagged
        mortality_dupes = pd.DataFrame()
    
    # Identify mother and child duplicates based on IDs (assuming mother_id and child_id are consistent)
    females_dupes = df_females[df_females.duplicated(subset="mother_id", keep=False)]
    preg_dupes = df_preg_history[df_preg_history.duplicated(subset="child_id", keep=False)]

    dupe_household_uuids = mortality_dupes['_uuid'].unique()
    dupe_mother_uuids = females_dupes['_submission__uuid'].unique()
    dupe_child_uuids = preg_dupes['_submission__uuid'].unique()
    
    # ---------------------------------------------------------
    
    # Aggregate female-level data
    females_agg = df_females.groupby('_submission__uuid').agg({
        c_alive_col: 'sum', c_dead_col: 'sum', miscarriage_col: 'sum',
        boys_dead_col: 'sum', girls_dead_col: 'sum'
    }).reset_index()
    females_agg['total_children_died'] = females_agg[boys_dead_col].fillna(0) + females_agg[girls_dead_col].fillna(0)

    # Aggregate pregnancy history data
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
        # Miscarriage and Born Dead are grouped under Miscarriage_Abortion check
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
            
        # Duplication Errors
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
        # Display the server error from load_data and stop if dataframes are empty
        return 

    # ---------------- DYNAMIC COLUMN MAPPING ----------------
    LGA_COL = find_column_with_suffix(df_mortality, "lga") or "Confirm your LGA"
    WARD_COL = find_column_with_suffix(df_mortality, "ward") or "Confirm your ward"
    # This column now holds the Community NAME after the mapping in load_data()
    COMMUNITY_COL = find_column_with_suffix(df_mortality, "community") or "Confirm your community" 
    RA_COL = find_column_with_suffix(df_mortality, "name") or "Type in your Name"
    DATE_COL = "start"
    VALIDATION_COL = "_validation_status"
    # *** UPDATED: Use the specific column name/search term for the unique code ***
    UNIQUE_CODE_COL_RAW = find_column_with_suffix(df_mortality, "unique_code") or find_column_with_suffix(df_mortality, "unique") or 'unique_code' 
    CONSENT_DATE_COL_RAW = find_column_with_suffix(df_mortality, "consent_date") or DATE_COL
    
    # ------------------ COLUMN DISPLAY NAMES ---------------------
    COMMUNITY_DISPLAY_NAME = "Confirm your community"
    LGA_DISPLAY_NAME = "LGA" 
    WARD_DISPLAY_NAME = "Ward" 
    RA_DISPLAY_NAME = "Enumerator Name"
    UNIQUE_CODE_DISPLAY_NAME = "unique_code" # *** UPDATED: Set desired display name ***
    # -------------------------------------------------------------------

    # --- Sidebar Filters ---
    with st.sidebar:
        st.header("Data Filters")
        # Use the raw column name for caption/dynamic lookup
        st.caption(f"LGA: `{LGA_COL}` | RA: `{RA_COL}` | Unique ID Col: `{UNIQUE_CODE_COL_RAW}`")
        st.markdown("---")

        # Safely determine if columns exist for filtering
        lga_filter_ok = LGA_COL in df_mortality.columns
        ward_filter_ok = WARD_COL in df_mortality.columns
        community_filter_ok = COMMUNITY_COL in df_mortality.columns
        ra_filter_ok = RA_COL in df_mortality.columns
        
        def apply_filters(df):
            df_filtered = df.copy()
            # LGA Filter
            if lga_filter_ok:
                selected_lga = st.selectbox("LGA", ["All"] + sorted(df_filtered[LGA_COL].dropna().unique()))
                if selected_lga != "All":
                    df_filtered = df_filtered[df_filtered[LGA_COL] == selected_lga]

            # Ward Filter
            if ward_filter_ok:
                selected_ward = st.selectbox("Ward", ["All"] + sorted(df_filtered[WARD_COL].dropna().unique()))
                if selected_ward != "All":
                    df_filtered = df_filtered[df_filtered[WARD_COL] == selected_ward]
            
            # Community Filter
            if community_filter_ok:
                selected_community = st.selectbox(COMMUNITY_DISPLAY_NAME, ["All"] + sorted(df_filtered[COMMUNITY_COL].dropna().unique()))
                if selected_community != "All":
                    df_filtered = df_filtered[df_filtered[COMMUNITY_COL] == selected_community]

            # RA Filter
            if ra_filter_ok:
                selected_ra = st.selectbox("Research Assistant", ["All"] + sorted(df_filtered[RA_COL].dropna().unique()))
                if selected_ra != "All":
                    df_filtered = df_filtered[df_filtered[RA_COL] == selected_ra]

            if DATE_COL in df_filtered.columns:
                try:
                    df_filtered[DATE_COL] = pd.to_datetime(df_filtered[DATE_COL], errors='coerce')
                    unique_dates = ["All"] + sorted(df_filtered[DATE_COL].dropna().dt.date.unique())
                    selected_date = st.selectbox("Collection Date", unique_dates)
                    if selected_date != "All":
                        df_filtered = df_filtered[df_filtered[DATE_COL].dt.date == selected_date]
                except Exception:
                    # Silent fail if date parsing fails
                    pass
            return df_filtered

        # Store the original unfiltered data for "Not Approved" table later
        df_mortality_original = df_mortality.copy()
        
        # Filter the main dataframe used for metrics/QC
        filtered_final = apply_filters(df_mortality)
        
        # Exclude "Not Approved" records from the main dashboard metrics view
        if VALIDATION_COL in filtered_final.columns:
            df_for_metrics = filtered_final[filtered_final[VALIDATION_COL] != "Not Approved"].copy()
            df_for_metrics[VALIDATION_COL].fillna("Validation Ongoing", inplace=True)
        else:
            df_for_metrics = filtered_final.copy()


    submission_ids = df_for_metrics['_uuid'].unique()
    filtered_females = df_females[df_females['_submission__uuid'].isin(submission_ids)]
    filtered_preg = df_preg[df_preg['_submission__uuid'].isin(submission_ids)]

    # Generate QC data for the full set, then filter by submission_ids
    df_qc = generate_qc_dataframe(df_mortality, df_females, df_preg)
    filtered_df = df_qc[df_qc['_submission__uuid'].isin(df_for_metrics['_uuid'])]

    # --- Dashboard Title & Metrics ---
    st.markdown('<div class="big-title">SARMAAN II - QC Dashboard - Cluster1</div>', unsafe_allow_html=True)
    st.caption("Data Quality Control and Monitoring")

    st.subheader("🎯 Operational Metrics")
    with st.container():
        cols = st.columns(4)
        cols[0].metric("Total Households Reached", f"{df_for_metrics['_uuid'].nunique():,}")
        
        ra_col_for_metric = RA_COL if RA_COL in df_for_metrics.columns else None
        ward_col_for_metric = WARD_COL if WARD_COL in df_for_metrics.columns else None
        community_col_for_metric = COMMUNITY_COL if COMMUNITY_COL in df_for_metrics.columns else None

        cols[1].metric("Active Enumerators", df_for_metrics[ra_col_for_metric].nunique() if ra_col_for_metric else 0)
        cols[2].metric("Wards Reached", df_for_metrics[ward_col_for_metric].nunique() if ward_col_for_metric else 0)
        cols[3].metric("Communities Reached", df_for_metrics[community_col_for_metric].nunique() if community_col_for_metric else 0)


    # ---------------- QC Summary ----------------
    st.subheader("🚨 Quality Control Summary (Metrics exclude 'Not Approved')")
    with st.container():
        cols = st.columns(6)
        
        born_alive_mismatch = (filtered_df["QC_Issues"].str.contains("Born Alive mismatch")).sum()
        later_died_mismatch = (filtered_df["QC_Issues"].str.contains("Born Alive but Later Died mismatch")).sum()
        miscarriage_mismatch = (filtered_df["QC_Issues"].str.contains("Miscarrage mismatch")).sum()
        
        display_qc_metric(cols[0], "Duplicate Household", (filtered_df["QC_Issues"].str.contains("Duplicate Household")).sum())
        display_qc_metric(cols[1], "Duplicate Mother", (filtered_df["QC_Issues"].str.contains("Duplicate Mother")).sum())
        display_qc_metric(cols[2], "Duplicate Child", (filtered_df["QC_Issues"].str.contains("Duplicate Child")).sum())
        display_qc_metric(cols[3], "Born Alive Mismatch", born_alive_mismatch)
        display_qc_metric(cols[4], "B.Alive, Later Died Mismatch", later_died_mismatch)
        display_qc_metric(cols[5], "Miscarriage Mismatch", miscarriage_mismatch)

    # ---------------- Not Approved Table ----------------
    st.markdown("---")
    st.subheader("❌ Submissions **NOT APPROVED** (Action: Recollection Required)")
    
    if VALIDATION_COL in df_mortality_original.columns:
        not_approved_df = df_mortality_original[df_mortality_original[VALIDATION_COL] == "Not Approved"].copy()

        if not not_approved_df.empty:
            not_approved_df['Validation Status'] = "Not Approved"
            
            display_cols = ['Validation Status', UNIQUE_CODE_COL_RAW, RA_COL, LGA_COL, WARD_COL, COMMUNITY_COL, DATE_COL]
            display_cols = [col for col in display_cols if col in not_approved_df.columns]
            
            display_na_df = not_approved_df[display_cols].rename(columns={
                UNIQUE_CODE_COL_RAW: UNIQUE_CODE_DISPLAY_NAME, # *** UPDATED DISPLAY NAME ***
                RA_COL: RA_DISPLAY_NAME,
                LGA_COL: LGA_DISPLAY_NAME,
                WARD_COL: WARD_DISPLAY_NAME,
                COMMUNITY_COL: COMMUNITY_DISPLAY_NAME, 
                DATE_COL: 'Submission Date'
            })
            
            if 'Submission Date' in display_na_df.columns:
                 display_na_df['Submission Date'] = pd.to_datetime(
                     display_na_df['Submission Date'], errors='coerce'
                 ).dt.strftime('%Y-%m-%d %H:%M')
            
            st.dataframe(display_na_df, use_container_width=True, height=300)
            st.error(f"🔴 **Total Not Approved:** {len(display_na_df):,} submissions must be revisited/recollected.")

        else:
            st.info("✅ No 'Not Approved' submissions found in the current filter selection.")
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

    # ---------------- DUPLICATE HOUSEHOLD RECORDS ----------------
    st.subheader("🏠 Duplicate Household Submissions (Excluding 'Not Approved')")
    
    if UNIQUE_CODE_COL_RAW in df_for_metrics.columns:
        dupe_mask = df_for_metrics.duplicated(subset=UNIQUE_CODE_COL_RAW, keep=False)
        duplicate_households = df_for_metrics[dupe_mask].sort_values(by=UNIQUE_CODE_COL_RAW).copy()

        if not duplicate_households.empty:
            display_dupe_cols = [
                '_uuid', UNIQUE_CODE_COL_RAW, RA_COL, LGA_COL, WARD_COL, 
                COMMUNITY_COL, DATE_COL
            ]
            display_dupe_cols = [col for col in display_dupe_cols if col in duplicate_households.columns]
            
            display_dupe_df = duplicate_households[display_dupe_cols].rename(columns={
                '_uuid': 'Submission UUID',
                UNIQUE_CODE_COL_RAW: UNIQUE_CODE_DISPLAY_NAME, # *** UPDATED DISPLAY NAME ***
                RA_COL: RA_DISPLAY_NAME,
                LGA_COL: LGA_DISPLAY_NAME,
                WARD_COL: WARD_DISPLAY_NAME,
                COMMUNITY_COL: COMMUNITY_DISPLAY_NAME,
                DATE_COL: 'Submission Date',
            })
            
            if 'Submission Date' in display_dupe_df.columns:
                 display_dupe_df['Submission Date'] = pd.to_datetime(
                     display_dupe_df['Submission Date'], errors='coerce'
                 ).dt.strftime('%Y-%m-%d %H:%M')

            # Update the warning message to reflect the new display name
            st.dataframe(display_dupe_df, use_container_width=True, height=300)
            st.warning(f"❗ **{len(display_dupe_df):,}** submissions share the same **{UNIQUE_CODE_DISPLAY_NAME}**. They should be reviewed.")
        else:
            st.info("✅ No duplicate household submissions found in the current filter selection.")
    else:
        st.error(f"❌ Cannot check for household duplicates. Unique Code column ('{UNIQUE_CODE_COL_RAW}') not found.")


    # ---------------- Detailed Error Records ----------------
    st.markdown("---")
    st.subheader("📋 Detailed Internal/Cross-Check Error Records (Excluding 'Not Approved')")
    display_df = filtered_df[filtered_df['Total_Flags'] > 0].copy()
    
    dupe_cols = ['_uuid', UNIQUE_CODE_COL_RAW, CONSENT_DATE_COL_RAW, VALIDATION_COL, LGA_COL, WARD_COL, COMMUNITY_COL, RA_COL]
    present_dupe_cols = [col for col in dupe_cols if col in df_for_metrics.columns]
    dupe_df = df_for_metrics[present_dupe_cols].rename(columns={'_uuid': '_submission__uuid'}).copy()
    
    if RA_COL in dupe_df.columns:
        dupe_df.rename(columns={RA_COL: 'Research_Assistant_Merge'}, inplace=True)

    if CONSENT_DATE_COL_RAW in dupe_df.columns and pd.api.types.is_datetime64_any_dtype(dupe_df[CONSENT_DATE_COL_RAW]):
        dupe_df[CONSENT_DATE_COL_RAW] = dupe_df[CONSENT_DATE_COL_RAW].dt.strftime('%Y-%m-%d')
        
    display_df = display_df.merge(dupe_df, on="_submission__uuid", how="left")
    display_df.drop(columns=["Research_Assistant_Merge"], inplace=True, errors='ignore')

    display_df.rename(columns={
        LGA_COL: LGA_DISPLAY_NAME, WARD_COL: WARD_DISPLAY_NAME, COMMUNITY_COL: COMMUNITY_DISPLAY_NAME,
        'Total_Flags': 'Total Flags', 'Error_Percentage': 'Error %', '_submission__uuid': 'Submission UUID',
        UNIQUE_CODE_COL_RAW: UNIQUE_CODE_DISPLAY_NAME, # *** UPDATED DISPLAY NAME ***
        CONSENT_DATE_COL_RAW: 'Date of Consent',
        VALIDATION_COL: 'Validation Status', 'Research_Assistant': RA_DISPLAY_NAME 
    }, inplace=True)
    
    display_cols = [
        'Submission UUID', UNIQUE_CODE_DISPLAY_NAME, RA_DISPLAY_NAME, 'Total Flags', 
        'Error %', 'QC_Issues', LGA_DISPLAY_NAME, WARD_DISPLAY_NAME, COMMUNITY_DISPLAY_NAME, 
        'Date of Consent', 'Validation Status'
    ]
    display_cols = [col for col in display_cols if col in display_df.columns]
    
    if not display_df.empty:
        st.dataframe(display_df[display_cols], use_container_width=True, height=500)
    else:
        st.info("🎉 No internal or cross-check errors found in the current filtered data.")


# ---------------- FORCE REFRESH BUTTON ----------------
with st.sidebar:
    st.markdown("---")
    if st.button("🔄 Force Refresh Data"):
        st.session_state.refresh = True
        # Rerunning the script will trigger the load_data with force_refresh=True
        st.rerun()

# ---------------- INITIAL LOAD ----------------
force_refresh_flag = st.session_state.get('refresh', False)
df_mortality, df_females, df_preg = load_data(force_refresh=force_refresh_flag)
st.session_state.refresh = False # Reset the flag after loading
run_dashboard(df_mortality, df_females, df_preg)
