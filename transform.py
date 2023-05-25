import extract
import pandas as pd
import os

def Data_Quality(load_df, column_name):
    #Checking Whether the DataFrame is empty
    if load_df.empty:
        print('No Songs Extracted')
        return False
    
    #Enforcing Primary keys since we don't need duplicates
    if column_name not in load_df.columns:
        print(f"{column_name} column does not exist in the DataFrame.")
        return False
    
    if pd.Series(load_df[column_name]).is_unique:
       return True
    else:
        #The Reason for using exception is to immediately terminate the program and avoid further processing
        raise Exception("Primary Key Exception,Data Might Contain duplicates")

def Transform_df(load_df):
    #drop duplicate songs
    Transformed_df=load_df.groupby(['timestamp', 'artist_name', 'song_name', 'track_id'],as_index = False).count()
    Transformed_df.rename(columns ={'played_at':'count'}, inplace=True)

    #Creating a Primary Key based on Timestamp and artist name
    Transformed_df["unique_identifier"] = Transformed_df['track_id'].astype(str) + "-" + Transformed_df["timestamp"].astype(str)

    return Transformed_df[['unique_identifier','track_id', 'artist_name', 'song_name', 'timestamp']]


if __name__ == "__main__":
    
    #Importing the recently played songs_df from the extract.py
    recently_played_df = extract.get_recently_played_data()
    top_songs_df, top_artists_df = extract.get_top_artists()
    track_ids = top_songs_df.track_id
    features_df = extract.get_features_df(track_ids)
   
    #calling the transformation for recently played songs df
    Transformed_df=Transform_df(recently_played_df) 
    
    #Save to filepath
    Transformed_df.to_csv(os.path.join('data/', "recently_played_df.csv"))
    top_songs_df.to_csv(os.path.join('data/', "top_songs_df.csv"))
    top_artists_df.to_csv(os.path.join('data/', "top_artists_df.csv"))
    features_df.to_csv(os.path.join('data/', "features_df.csv"))
    
   
