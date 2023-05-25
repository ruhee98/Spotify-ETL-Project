import extract
import transform
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import sqlite3
from sqlite3 import Error
import pandas as pd


def create_connection(db):
        try:
            engine = sqlalchemy.create_engine(db)
            conn = sqlite3.connect('spotify_db.db')
            return conn, engine
        except Error as e:
            print(e)
    
def create_table(conn, create_table_query):
             
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        print("Created Tables successfully")
    except Error as e:
        print(e)
    
    
def join_table(conn, join_table_query):
    try:
        cursor = conn.cursor()
        cursor.execute(join_table_query)
        print("Joined tables")
        
        rows = cursor.fetchall()
        return rows
    except Error as e:
        print(e)
    

if __name__ == "__main__":

    recently_played_df = extract.get_recently_played_data()
    top_songs_df, top_artists_df = extract.get_top_artists()
    track_ids = top_songs_df.track_id
    features_df = extract.get_features_df(track_ids)
    
    transform.Data_Quality(top_songs_df, 'track_id')
    transform.Data_Quality(top_artists_df, 'artist_id')
    transform.Data_Quality(features_df, 'track_id')
    
    #Transformation function
    if(transform.Data_Quality(recently_played_df, 'played_at') == False):
        raise ("Failed at Data Validation")
    
    recently_played_df = transform.Transform_df(recently_played_df)
    
    #Creating connection
    db = 'sqlite:///spotify_db.db'
    conn, engine = create_connection(db)

    recently_played_table = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        unique_identifier VARCHAR(200),
        track_id VARCHAR(200),
        artist_name VARCHAR(200),
        song_name VARCHAR(200),
        timestamp TIMESTAMP, 
        CONSTRAINT primary_key_constraint PRIMARY KEY (unique_identifier)
    )
    """

    top_songs_table = """
    CREATE TABLE IF NOT EXISTS my_top_songs(
        track_id VARCHAR(200), 
        song_name VARCHAR(200),
        artist_name VARCHAR(200), 
        album_name VARCHAR(200), 
        album_release_date VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (track_id)
    )
    """
    
    features_table = """
    CREATE TABLE IF NOT EXISTS track_features(
        track_id VARCHAR(200), 
        danceability INTEGER, 
        energy INTEGER,
        loudness INTEGER,
        mode INTEGER,
        acousticness INTEGER,
        instrumentalness INTEGER,
        speechiness INTEGER,
        liveness INTEGER,
        tempo INTEGER,
        valence INTEGER,
        CONSTRAINT primary_key_constraint PRIMARY KEY (track_id)       
    )
    """
    
    join_tables = """
        SELECT * FROM my_top_songs as ts
        INNER JOIN track_features as tf
        ON ts.track_id = tf.track_id
    """
    
    if conn is not None:
        create_table(conn, recently_played_table)
        create_table(conn, top_songs_table)
        create_table(conn, features_table)
        joined_table = join_table(conn, join_tables)
        joined_df = pd.DataFrame(joined_table, columns=['track_id', 'song_name', 'artist_name', 'album_name', 'release_date', 'track_id_2', 'danceability', 
                                                        'energy', 'loudness', 'mode', 'acousticness', 'instrumentalness', 'speechiness', 'liveness', 
                                                        'tempo', 'valence'])
        joined_df = joined_df.drop(['track_id_2'],axis=1)
    else:
        print("Error! cannot create the database connection.")
    
    conn.close()
    print("Database closed")

    try:
        recently_played_df.to_sql("my_played_tracks", engine, index=False, if_exists='replace')
        top_songs_df.to_sql('my_top_songs', engine, index=False, if_exists='replace')
        features_df.to_sql('track_features', engine, index=False, if_exists='replace')
        joined_df.to_sql('top_songs_with_features', engine, index=False, if_exists='replace')
        
    except Exception as e:
        print(e)
    

    


