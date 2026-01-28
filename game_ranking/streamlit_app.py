import streamlit as st
import pandas as pd
import numpy as np
from src.calculation.process_data import clean_dev_genre_list, flagging, calculate_developer_weighted_points, load_data, calculate_follower_weighted_points, calculate_developer_weighted_points
from config import CSV_STEAM, CSV_NON_STEAM

# Page Config
st.set_page_config(page_title="AGS - Game Ranking Tool", layout="wide")

st.title("🎮 Research Team: Game Ranking Algorithm")
st.markdown("""
This tool calculates review priority by awarding 'points' to Steam and Non-Steam games
            based on certain criteria, and weights to determine the final recommendation./n
            
            The criteria for each report are displayed on the sidebar, and their weights can be adjusted
            """)

# LOADING DATA
try:
    df_steam, df_nonsteam, dev_list, genre_list = load_data(CSV_STEAM, CSV_NON_STEAM)
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# SET UP DIFFERENT TABS FOR STEAM AND NON-STEAM REPORTS
tab_steam , tab_nonsteam = st.tabs(["🚀 Steam Report", "📽️ Non-Steam Report"])

# TAB 1: STEAM REPORT
with tab_steam:
    st.header("Steam Game Ranking")
    
    # Sidebar Inputs for Steam
    st.sidebar.header("Steam Report Configuration")
    
    # 1. Follower Point Milestone
    min_followers = st.sidebar.number_input("Min Followers", value=10000, help="Every X followers equals 1 point")
    max_followers = st.sidebar.number_input("Max Followers", value=398955, help="Every X followers equals 1 point")
    
    # 2. Weights
    w_followers = st.sidebar.slider("Follower Weight", 0, 5, 5)
    w_developers = st.sidebar.slider("Developer Weight", 0, 5, 2)
    
    # 3. clean df and flag data
    df_steam = clean_dev_genre_list(df_steam)
    df_steam = flagging(df_steam)

    # 4. Follower Points Calculation Logic
    st.info("### Current Steam Formula")
    st.latex(r"Follower Points = (0.5*linear + 0.5*log)")
    st.latex(r"Developer Points = (Avg.of Developer Points)")
    st.latex(r"Final Priority Score = ((Follower Points * Follower Weight) + (Developer Points * Developer Weight)")
    st.caption("*_The above formula is based on points from the Developer List_")
    st.caption("**_If any developer is not in the Developer List, they are assigned a default point value of 1._")
    # Calculate follower_points for each game
    follower_points = []
    for index, row in df_steam.iterrows():
        followers = row['FollowerCount']
        points = calculate_follower_weighted_points(followers, min_followers=min_followers, max_followers=max_followers)
        follower_points.append(points)
        df_steam.loc[index, 'Follower Points'] = points

    # Display Formula

    # 5. Developer Points Calculation Logic
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

    # Sort for the team
    df_ranked = df_steam.sort_values('Final Priority Score', ascending=False)

    # 4. Display Results
    tab1, tab2 = st.tabs(["📊 Ranking Results", "🔍 Developer List"])

    with tab1:
        st.subheader("Top Priority Games")
        # Clean view for the team
        cols_to_show = ['Name', 'FollowerCount', 'Follower Points', 'Developers', 'Developer Points', 'Final Priority Score']
        st.dataframe(df_ranked[cols_to_show], use_container_width=True)

    with tab2:
        st.subheader("Developer Ranking List")
        st.info("Below is the internal ranking list for developers based on their Average Revenue per Game:")
        # Mathematical explanation
        st.dataframe(dev_list, use_container_width=True)