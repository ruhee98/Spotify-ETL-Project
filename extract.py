import pandas as pd 
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util
import requests
from datetime import datetime
import datetime

username='1289998887'
client_id = 'ea6b055b3b974015a8585f70ec64a127'
client_secret = 'd0a2e88eb81d4cf1a879086f15ac0803'
redirect_uri = 'http://localhost:7777/callback'
scopes = ['user-read-recently-played', 'user-top-read', 'playlist-read-collaborative', 'playlist-read-private', 'playlist-modify-public', 'playlist-modify-private']

#Get top artists

bearer_token = util.prompt_for_user_token(username=username, 
                                   scope=scopes, 
                                   client_id=client_id,   
                                   client_secret=client_secret,     
                                   redirect_uri=redirect_uri)

headers = {
        "Authorization" : "Bearer {token}".format(token=bearer_token)
}


#Recently Played 
def get_recently_played_data():
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=3)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    recently_played = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers=headers)
    data = recently_played.json()
    track_ids = []
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    
    for song in data["items"]:
        track_ids.append(song["track"]["id"])
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
    
    song_dict = {
        "track_id": track_ids,
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }
    song_df = pd.DataFrame(song_dict, columns = ["track_id", "song_name", "artist_name", "played_at", "timestamp"])
    return song_df

# song_df = get_recently_played_data()

def get_top_artists():
    top_artists = requests.get('https://api.spotify.com/v1/me/top/artists', headers=headers)
    top_tracks = requests.get('https://api.spotify.com/v1/me/top/tracks', headers=headers)
    
    data1 = top_artists.json()
    data2 = top_tracks.json()
    artist_names = []
    song_names = []
    song_artist_names = []
    genres = []
    album_names = []
    album_names = []
    album_release = []
    track_ids = []
    artist_ids = []

    for artist in data1['items']:
        artist_ids.append(artist['id'])
        artist_names.append(artist['name'])
        genres.append(artist['genres'])

    for track in data2["items"]:
        track_ids.append(track['id'])
        song_names.append(track['name'])
        album_names.append(track["album"]["name"])
        album_release.append(track["album"]["release_date"])
        song_artist_names.append(track['album']['artists'][0]['name'])
    
    song_dict = {
        "track_id" : track_ids,
        "song_name" : song_names,
        "artist_name": song_artist_names,
        "album_name" : album_names,
        "album_release_date" : album_release
    }
    
    artist_dict = {
        "artist_id": artist_ids,
        "artist_name": artist_names,
        "genre": genres
    }
    
    song_df = pd.DataFrame(song_dict, columns = ["track_id", "song_name", "artist_name", "album_name", "album_release_date"])
    artist_df = pd.DataFrame(artist_dict, columns=['artist_id', 'artist_name', 'genre'])
    return song_df, artist_df

top_songs, top_artists = get_top_artists()


def get_features(track_id:str):
    params = {
        'ids': track_id
    }
    response = requests.get('https://api.spotify.com/v1/audio-features', params=params, headers=headers)
    
    data = response.json()
    
    for f in data['audio_features']:
        feature_dict = {
            'danceability': f['danceability'],
            'energy': f['energy'],
            'loudness': f['loudness'],
            'mode': f['mode'],
            'acousticness': f['acousticness'],
            'instrumentalness': f['instrumentalness'],
            'speechiness': f['speechiness'],
            'liveness': f['liveness'],
            'tempo':f['tempo'],
            'valence': f['valence']
        }
    
    return feature_dict

def get_features_df(track_ids:str):
    feature_list = []
    for track_id in track_ids:
        track_id_dict = {'track_id': track_id}
        feature_dict = get_features(track_id)
        feature_dict.update(track_id_dict)
        feature_list.append(feature_dict)
    
    track_features_df = pd.DataFrame(feature_list, columns=['track_id', 'danceability', 'energy', 'loudness', 'mode', 'acousticness', 'instrumentalness', 'speechiness', 'liveness', 'tempo', 'valence'])

    return track_features_df

# track_features_df = get_features_df(track_ids)

# def get_followed_playlist():
    