import requests
import json
import pandas as pd
from datetime import date, timedelta, datetime

import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"
MAXRESULT = 50

def get_playlist_id():
    
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        channel_items = data['items'][0]
        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']
        
        return channel_playlistId
    
    except requests.exceptions.RequestException as e:
        raise e
    
def get_video_id(playlist_id):

    video_id_list = []
    pageToken = None

    params = {
        "part": "contentDetails",
        "maxResults": MAXRESULT,
        "playlistId": playlist_id,
        "key": API_KEY

    }

    url = "https://youtube.googleapis.com/youtube/v3/playlistItems"

    try:

        while True:

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_id_list.append(video_id)

            print(f"fetch so far {len(video_id_list)} so far...")    

            if "nextPageToken" in data:
                params["pageToken"] = data["nextPageToken"]
            else:
                break;         

        return video_id_list
    
    except requests.exceptions.RequestException as e:
        raise e


if __name__ == "__main__":
    playlist_id = get_playlist_id()
    print(playlist_id)
    video_list = get_video_id(playlist_id)
    print(video_list[:100])

# adding comments 

