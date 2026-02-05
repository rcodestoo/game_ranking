import streamlit as st
import pandas as pd
import numpy as np
import io
import datetime as dt
from src.calculation.process_data import clean_dev_genre_list, flagging, calculate_developer_weighted_points, load_data, calculate_follower_weighted_points, calculate_developer_weighted_points, handle_change
from config import CSV_STEAM, CSV_NON_STEAM, INVENTORY_FILE
# from st_aggrid import AgGrid

# Page Config
st.set_page_config(page_title="AGS - Game Ranking Tool", layout="wide")

st.title("🎮 Research Team: Game Ranking Algorithm")
st.markdown("""
This tool calculates review priority by awarding 'points' to Steam and Non-Steam games
            based on certain criteria, and weights to determine the final recommendation.
            
            The criteria for each report are displayed on the sidebar, and their weights can be adjusted.

**Instructions:**
1. **Upload your own CSV files** for Steam and Non-Steam reports using the sidebar file uploaders. If no files are uploaded, default files will be used.
2. **Preview the uploaded files** before loading to ensure they have the correct format and columns.
3. **Adjust the criteria weights** in the sidebar to see how they affect the rankings.  
            """)

# SIDEBAR: FILE UPLOADS
st.sidebar.header("📁 Data Upload")
st.sidebar.markdown("Upload your own CSV files or use the defaults below:")

# Initialize session state for data caching
if "df_steam" not in st.session_state:
    st.session_state.df_steam = None
if "df_nonsteam" not in st.session_state:
    st.session_state.df_nonsteam = None
if "steam_cleaned" not in st.session_state:
    st.session_state.steam_cleaned = False
if "nonsteam_cleaned" not in st.session_state:
    st.session_state.nonsteam_cleaned = False
if "uploaded_steam_bytes" not in st.session_state:
    st.session_state.uploaded_steam_bytes = None
if "uploaded_steam_name" not in st.session_state:
    st.session_state.uploaded_steam_name = None
if "uploaded_nonsteam_bytes" not in st.session_state:
    st.session_state.uploaded_nonsteam_bytes = None
if "uploaded_nonsteam_name" not in st.session_state:
    st.session_state.uploaded_nonsteam_name = None
if "dev_list" not in st.session_state:
    try:
        _, _, st.session_state.dev_list, st.session_state.genre_list = load_data(CSV_STEAM, CSV_NON_STEAM)
    except:
        pass

# Helper: load defaults into session state
def load_defaults():
    df_steam, df_nonsteam, dev_list, genre_list, inventory = load_data(CSV_STEAM, CSV_NON_STEAM)
    df_steam = clean_dev_genre_list(df_steam)
    df_steam = flagging(df_steam)
    st.session_state.df_steam = df_steam
    st.session_state.steam_source = "default file"
    st.session_state.steam_cleaned = True
    st.session_state.df_nonsteam = df_nonsteam
    st.session_state.nonsteam_source = "default file"
    st.session_state.nonsteam_cleaned = True
    st.session_state.dev_list = dev_list
    st.session_state.genre_list = genre_list
    # st.session_state.game_data = inventory
    # Clear cached upload bytes
    st.session_state.uploaded_steam_bytes = None
    st.session_state.uploaded_steam_name = None
    st.session_state.uploaded_nonsteam_bytes = None
    st.session_state.uploaded_nonsteam_name = None

# File uploaders
uploaded_steam = st.sidebar.file_uploader("Upload Steam CSV", type="csv", key="steam_upload")
uploaded_nonsteam = st.sidebar.file_uploader("Upload Non-Steam CSV", type="csv", key="nonsteam_upload")

if uploaded_steam and uploaded_steam.name != st.session_state.uploaded_steam_name:
    st.session_state.uploaded_steam_bytes = uploaded_steam.getvalue()
    st.session_state.uploaded_steam_name = uploaded_steam.name

if uploaded_nonsteam and uploaded_nonsteam.name != st.session_state.uploaded_nonsteam_name:
    st.session_state.uploaded_nonsteam_bytes = uploaded_nonsteam.getvalue()
    st.session_state.uploaded_nonsteam_name = uploaded_nonsteam.name

# Preview and load buttons for Steam
if st.session_state.uploaded_steam_bytes:
    with st.sidebar.expander("👀 Preview Steam File"):
        preview_steam = pd.read_csv(io.BytesIO(st.session_state.uploaded_steam_bytes))
        st.dataframe(preview_steam.head(3), use_container_width=True)
        st.caption(f"Rows: {len(preview_steam)}, Columns: {len(preview_steam.columns)}")
    
    if st.sidebar.button("📥 Load Steam Data", key="load_steam_btn"):
        try:
            steam_df_upload = pd.read_csv(io.BytesIO(st.session_state.uploaded_steam_bytes))
            steam_required_cols = ['Name', 'FollowerCount', 'Developers', 'Genres', 'ReleaseDate']
            steam_missing = [col for col in steam_required_cols if col not in steam_df_upload.columns]
            if steam_missing:
                st.sidebar.error(f"Missing columns: {', '.join(steam_missing)}")
            else:
                # Clean the data ONCE before storing in session state
                steam_df_upload = clean_dev_genre_list(steam_df_upload)
                steam_df_upload = flagging(steam_df_upload)
                
                st.session_state.df_steam = steam_df_upload
                st.session_state.steam_source = st.session_state.uploaded_steam_name
                st.session_state.steam_cleaned = True
                st.sidebar.success(f"✅ Loaded {st.session_state.uploaded_steam_name}")
        except Exception as e:
            st.sidebar.error(f"Error loading file: {e}")
else:
    st.sidebar.info("No Steam CSV uploaded. Using default.")

# Preview and load buttons for Non-Steam
if st.session_state.uploaded_nonsteam_bytes:
    with st.sidebar.expander("👀 Preview Non-Steam File"):
        preview_nonsteam = pd.read_csv(io.BytesIO(st.session_state.uploaded_nonsteam_bytes))
        st.dataframe(preview_nonsteam.head(3), use_container_width=True)
        st.caption(f"Rows: {len(preview_nonsteam)}, Columns: {len(preview_nonsteam.columns)}")
    
    if st.sidebar.button("📥 Load Non-Steam Data", key="load_nonsteam_btn"):
        try:
            nonsteam_df_upload = pd.read_csv(io.BytesIO(st.session_state.uploaded_nonsteam_bytes))
            nonsteam_required_cols = ['Game Title', 'Developers', 'SteamStatus', 'YouTube Views']
            nonsteam_missing = [col for col in nonsteam_required_cols if col not in nonsteam_df_upload.columns]
            if nonsteam_missing:
                st.sidebar.error(f"Missing columns: {', '.join(nonsteam_missing)}")
            else:
                st.session_state.df_nonsteam = nonsteam_df_upload
                st.session_state.nonsteam_source = st.session_state.uploaded_nonsteam_name
                st.session_state.nonsteam_cleaned = True
                st.sidebar.success(f"✅ Loaded {st.session_state.uploaded_nonsteam_name}")
        except Exception as e:
            st.sidebar.error(f"Error loading file: {e}")
else:
    st.sidebar.info("No Non-Steam CSV uploaded. Using default.")

# Reset to defaults button
st.sidebar.divider()
if st.sidebar.button("🔄 Reset to Defaults"):
    load_defaults()
    st.rerun()

# Load defaults if not already loaded
try:
    if st.session_state.df_steam is None or st.session_state.df_nonsteam is None:
        load_defaults()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# Retrieve data from session state
df_steam = st.session_state.df_steam.copy()  # Use .copy() to avoid modifying session state directly
df_nonsteam = st.session_state.df_nonsteam.copy()
dev_list = st.session_state.dev_list
genre_list = st.session_state.genre_list
steam_source_name = st.session_state.get("steam_source", "default file")
nonsteam_source_name = st.session_state.get("nonsteam_source", "default file")

# SET UP DIFFERENT TABS FOR STEAM AND NON-STEAM REPORTS
tab_steam , tab_nonsteam, tab_inventory = st.tabs(["🚀 Steam Report", "📽️ Non-Steam Report", "🎮 Game Inventory"])

# TAB 1: STEAM REPORT
with tab_steam:
    st.header("Steam Game Ranking")
    st.caption(f"📊 Loading from {steam_source_name}")
    
    # Sidebar Inputs for Steam
    st.sidebar.header("Steam Report Configuration")
    
    # 1. Follower Point Milestone
    min_followers = st.sidebar.number_input("Min Followers", value=10000, help="Min followers to be considered for points")
    max_followers = st.sidebar.number_input("Max Followers", value=398955, help="Max followers to be considered for points")
    
    # 2. Weights
    w_followers = st.sidebar.slider("Follower Weight", 0, 5, 5)
    w_developers = st.sidebar.slider("Developer Weight", 0, 5, 2)

    # Steam Rankings Calculation Logic
    st.info("### Current Steam Formula")
    st.latex(r"1. Follower Points = (0.5 \times linear\_norm) + (0.5 \times log\_norm)")
    # Detailed explanation in case needed  
    with st.expander("📐 View Linear & Log Details"):
        st.latex(r'linear\_norm = \frac{followers - min\_followers}{max\_followers - min\_followers} \times (5 - 1) + 1')
        st.latex(r'log\_norm = \frac{\log(followers) - \log(min\_followers)}{\log(max\_followers) - \log(min\_followers)} \times (5 - 1) + 1')

    st.latex(r"2. Developer Points = (Avg.of Developer Points)")
    st.latex(r"3. Final Priority Score = ((Follower Points * Follower Weight) + (Developer Points * Developer Weight)")
    st.caption("*_The above formula is based on points from the Developer List_")
    st.caption("**_If any developer is not in the Developer List, they are assigned a default point value of 1._")
    # Calculate follower_points for each game
    follower_points = []
    for index, row in df_steam.iterrows():
        followers = row['FollowerCount']
        points = calculate_follower_weighted_points(followers, min_followers=min_followers, max_followers=max_followers)
        follower_points.append(points)
        df_steam.loc[index, 'Follower Points'] = points

    # Developer Points Calculation Logic
    developer_points = []
    for index, row in df_steam.iterrows():
        developers = row['Developers']
        points, missing_devs = calculate_developer_weighted_points(developers)
        developer_points.append(points)
        df_steam.loc[index, 'Developer Points'] = points
    
    # Final Score Calculation
    df_steam['Weighted Follower Score'] = df_steam['Follower Points'] * w_followers
    df_steam['Weighted Dev Score'] = df_steam['Developer Points'] * w_developers
    df_steam['Final Priority Score'] = df_steam['Weighted Follower Score'] + df_steam['Weighted Dev Score']

    # Sorting games based on ranking
    df_ranked = df_steam.sort_values('Final Priority Score', ascending=False, ignore_index=True)
    df_ranked.index = df_ranked.index + 1

    # Display Results
    tab1, tab2 = st.tabs(["📊 Ranking Results", "🔍 Developer List"])

    with tab1:
        st.subheader("Top Priority Games")
        # Clean view for the team
        cols_to_show = ['Name', 'FollowerCount', 'Follower Points', 'Developers', 'Developer Points', 'Final Priority Score']

        df_display = df_ranked[cols_to_show].copy()

        # Convert Developers list to string
        df_display['Developers'] = df_display['Developers'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else str(x)
        )

        st.dataframe(df_display, use_container_width=True)
        # st.dataframe(df_ranked[cols_to_show], use_container_width=True)

    with tab2:
        st.subheader("Developer Ranking List")
        st.info("Below is the internal ranking list for developers based on their Average Revenue per Game:")
        # Mathematical explanation
        st.dataframe(dev_list, use_container_width=True)

# TAB 2: NON-STEAM REPORT
with tab_nonsteam:
    st.header("Non-Steam Game Ranking")
    st.caption(f"📊 Loading from {nonsteam_source_name}")

    # Sidebar Inputs for Non-Steam
    st.sidebar.header("Non-Steam Report Configuration")
    st.info("The Non-Steam ranking is based on YouTube Views adjusted for the time since release. Games with higher adjusted views are prioritized.")

    #Pre-processesing Data and filter out Steam Games
    df_nonsteam_filter = df_nonsteam[df_nonsteam['SteamStatus'] != 'PC Game (on Steam)']
    df_nonsteam_filter['YouTube ReleaseDate'] = pd.to_datetime(df_nonsteam_filter['YouTube ReleaseDate'], errors='coerce')

    #Calculation Logic for Non-Steam
    # To display: Non-Steam Rankings Calculation Logic
    st.info("### Current Non-Steam Formula")
    st.latex(r"1. Days since Release = (Today's Date - Release Date)")
    st.latex(r"2. Adjusted Views = YouTube Views / (1 + (Days Since Release / 365))")
    st.latex(r"Games with the highest Adjusted Views are ranked highest.")
    
    #Actual Calculation Logic 
    today = dt.date.today()
    df_nonsteam_filter['Days_Since_Release'] = (pd.to_datetime(today) - df_nonsteam_filter['YouTube ReleaseDate']).dt.days
    df_nonsteam_filter['adjusted_views'] = df_nonsteam_filter['YouTube Views'] / (1 + (df_nonsteam_filter['Days_Since_Release'] / 365))
    
    # Sorting games based on ranking
    df_non_steam_ranked = df_nonsteam_filter.sort_values('adjusted_views', ascending=False, ignore_index=True)
    df_non_steam_ranked.index = df_non_steam_ranked.index + 1
    
    # Display Results
    st.subheader("Top Priority Non-Steam Games")
    # Clean view for the team
    cols_to_show = ['Game Title', 'adjusted_views', 'YouTube Views', 'Days_Since_Release', 'Release Date', 'Developers', 
                    'Platforms', 'Genres',  'YouTube URL', 'YouTube ReleaseDate', 
                    'SteamStatus'] 
    df_nonsteam_display = df_non_steam_ranked[cols_to_show].copy()
    
    def format_list_column(value):
        """Convert list to comma-separated string, handling various input types"""
        if isinstance(value, list):
            # Filter out any None or empty values
            clean_values = [str(v).strip() for v in value if v and str(v).strip()]
            return ', '.join(clean_values)
        # elif isinstance(value, str):
        #     # If it's already a string, just return it
        #     return value
        else:
            return str(value)
    
    # Format Developers column if it exists
    if 'Developers' in df_nonsteam_display.columns:
        df_nonsteam_display['Developers'] = df_nonsteam_display['Developers'].apply(format_list_column)
    
    # Format Genres column if it exists
    if 'Genres' in df_nonsteam_display.columns:
        df_nonsteam_display['Genres'] = df_nonsteam_display['Genres'].apply(format_list_column)
    
    st.dataframe(df_nonsteam_display, use_container_width=True)
    #st.dataframe(df_non_steam_ranked[cols_to_show], use_container_width=True)

    #DATA INSIGHTS
    # st.bar_chart(df_nonsteam['SteamStatus'].value_counts())

    

    # with tab2:
    #     st.subheader("Developer Ranking List")
    #     st.info("Below is the internal ranking list for developers based on their Average Revenue per Game:")
    #     # Mathematical explanation
    #     st.dataframe(dev_list, use_container_width=True)

# TAB 3: GAME INVENTORY
with tab_inventory:
    st.title("📋 Interactive Sheet with Add Row Button")

    # Initialize session state
    if "game_data" not in st.session_state:
        st.session_state.game_data = pd.read_csv(INVENTORY_FILE)

    # Display instructions
    st.header("🎮 Game Tracker")
    st.subheader("Game Library")
    st.info("💡 Edit the table directly, toggle checkboxes, or use the + button to add rows. Changes save automatically!")

    # Data Insights
    st.divider()
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Total Games", len(st.session_state.game_data), border=True)

    with col2:
        active_count = int(st.session_state.game_data['Active'].sum())
        st.metric("Active", active_count, border=True)

    with col3:
        on_hold_count = int(st.session_state.game_data['On Hold'].sum())
        st.metric("On Hold", on_hold_count, border=True)

    with col4:
        reviewed_count = int(st.session_state.game_data['Reviewed'].sum())
        st.metric("Reviewed", reviewed_count, border=True)

    with col5:
        inactive_count = int(st.session_state.game_data['Inactive'].sum())
        st.metric("Inactive", inactive_count, border=True)

    st.divider()
    
    #create editable table with data_editor as key
    st.data_editor(
        st.session_state.game_data,
        num_rows="dynamic",
        key="game_editor",
        column_config={
            "Game Name": st.column_config.TextColumn("Game", width="medium", required=True),
            "Date Purchased": st.column_config.TextColumn("Date Purchased"),
            "Physical": st.column_config.CheckboxColumn("Physical"),
            "Digital": st.column_config.CheckboxColumn("Digital"),
            "Platform": st.column_config.TextColumn("Platform"),
            "Account": st.column_config.TextColumn("Account"),
            "Inactive": st.column_config.CheckboxColumn("Inactive"),
            "On Hold": st.column_config.CheckboxColumn("On Hold"),
            "Active": st.column_config.CheckboxColumn("Active"),
            "Reviewed": st.column_config.CheckboxColumn("Reviewed"),
            "Links": st.column_config.LinkColumn("Links", display_text="Open Link")
        },
        disabled=["Game Name"],  # Disable editing for Game Name to preserve data integrity
        on_change=handle_change,
    )

    