from pytrends.request import TrendReq
import pandas as pd

def scrape_google_trends(keywords, timeframe='today 12-m'):
    # Set up Pytrends connection
    pytrends = TrendReq(hl='en-US', tz=360)

    # Build payload with specified keywords and timeframe
    pytrends.build_payload(keywords, cat=0, timeframe=timeframe) #, geo='', gprop=''

    # Retrieve interest over time data
    interest_over_time_df = pytrends.interest_over_time()

    return interest_over_time_df

# # Set up Pytrends connection
# pytrends = TrendReq(hl='en-US', tz=360)

# # Define keywords and fetch data
# keywords = ['Indie', 'FPS'] #, 'Java', 'JavaScript']
# pytrends.build_payload(keywords, cat=0, timeframe='today 12-m', geo='', gprop='')

# # Retrieve interest over time
# interest_over_time_df = pytrends.interest_over_time()

# # Display data
# print(interest_over_time_df.head())

# # Save data to CSV (optional)
# interest_over_time_df.to_csv('google_trends_data.csv')