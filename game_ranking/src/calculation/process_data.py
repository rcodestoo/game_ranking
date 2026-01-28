import streamlit as st
import pandas as pd
import math
from config import CSV_STEAM, DEV_LIST, GENRE_LIST


#LOAD DEVELOPER AND GENRE LIST
developer_list = pd.read_excel(DEV_LIST)
genre_list = pd.read_excel(GENRE_LIST)

#FUNCTION TO LOAD DATA
@st.cache_data
def load_data(steam_report, non_steam_report, developer_list=developer_list, genre_list=genre_list):
    # Replace with your actual pre-processed CSV filename
    steam_df = pd.read_csv(steam_report)
    nonsteam_df = pd.read_csv(non_steam_report)
    return steam_df, nonsteam_df, developer_list, genre_list

#CLEANING DEV AND GENRE LIST
def clean_dev_genre_list(df):
    df['Developers'] = df['Developers'].astype(str)
    df['Genres'] = df['Genres'].astype(str)

    # Replace common suffixes to avoid splitting issues
    df['Developers'] = df['Developers'].str.replace(r',\s*inc\.?', ' Inc.', case=False, regex=True)
    df['Developers'] = df['Developers'].str.replace(r',\s*ltd\.?', ' Ltd.', case=False, regex=True)
    df['Developers'] = df['Developers'].str.replace(r',\s*llc\.?', ' LLC.', case=False, regex=True)

    # Split the string at the comma to create a list
    df['Developers'] = df['Developers'].str.split(',')
    df['Genres'] = df['Genres'].str.split(',')
    return df


#CREATING FLAGS TO IDENTIFY DATA WITH CERTAIN CRITERIA 
def flagging(df):
    #selecting columns for calculation
    df_calculation = df[["Name", "ReleaseDate", "Developers", "Genres", "FollowerCount"]]

    #Flagging for indie genre
    for index, row in df_calculation.iterrows():
        genres = row['Genres']
        indie_flag = False
        for genre in genres:
            if genre.strip().lower() == 'indie':
                indie_flag = True
                break
        df_calculation.loc[index, 'Is_Indie'] = indie_flag

        #flagging for multi devs and multi genres
        developers = row['Developers']
        multiple_developers_flag = False
        if len(developers) > 1:
            multiple_developers_flag = True
        df_calculation.loc[index, 'Has_Multiple_Developers'] = multiple_developers_flag
        genres = row['Genres']
        multiple_genres_flag = False
        if len(genres) > 1:
            multiple_genres_flag = True
        df_calculation.loc[index, 'Has_Multiple_Genres'] = multiple_genres_flag
    return df_calculation


#CALCULATING FOLLOWER POINTS
def calculate_follower_weighted_points(followers, min_followers, max_followers):
    """
    Calculate weighted points based on follower count using hybrid linear/logarithmic approach.
    Replicates the Excel formula: (0.5*linear + 0.5*log) * weight_factor
    
    Args:
        followers: Number of followers
        min_followers: Minimum threshold (1000)
        max_followers: Maximum threshold (398955)
    """
    
    # Linear normalization to 1-5 scale
    linear_norm = ((followers - min_followers) / (max_followers - min_followers)) * (5 - 1) + 1
    
    # Logarithmic normalization to 1-5 scale
    log_norm = ((math.log(followers) - math.log(min_followers)) / 
                (math.log(max_followers) - math.log(min_followers))) * (5 - 1) + 1
    
    # 50/50 weighted average of linear and logarithmic
    result = 0.5 * linear_norm + 0.5 * log_norm

    
    return result


#CALCULATING DEVELOPER WEIGHTED POINTS
def calculate_developer_weighted_points(developers, developer_list=developer_list):
    weighted_points=[]
    missing_devs = []
    dev_points = []
    for developer in developers:
        weighted_point = developer_list.loc[developer_list['Developer Name'].str.strip().str.lower() == developer.strip().lower(), 'Total Hybrid Weighted Points']
        if not weighted_point.empty:
            dev_points.append(weighted_point.iloc[0].item())
        else:
            dev_points.append(1)
            missing_devs.append(developer.strip())
    avg_weighted_point = sum(dev_points) / len(dev_points) if dev_points else 1
    return avg_weighted_point, missing_devs