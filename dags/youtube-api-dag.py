from __future__ import annotations

import requests
import json
import pandas as pd
from datetime import date, timedelta, datetime
from airflow.decorators import dag, task
# from pyspark.sql import SparkSession


import os
# from dotenv import load_dotenv
# load_dotenv(dotenv_path="./.env")

API_KEY_ENV_NAME = "YOUTUBE_API_KEY"
API_KEY = os.getenv(API_KEY_ENV_NAME)

# API_KEY = os.getenv("API_KEY")
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

def extract_video_data(video_id_list):

    extracted_list = []

    try:
        
        for start in range(0, len(video_id_list), MAXRESULT):
            batch = video_id_list[start:start+MAXRESULT]
            video_ids_str = ",".join(batch)

            params = {
            "part": "contentDetails,snippet,statistics",
            "id": video_ids_str,
            "key": API_KEY
            
            }


            url = "https://youtube.googleapis.com/youtube/v3/videos"

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            for item in data.get('items', []):
                
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]

                video_data = {
                    "video_id": item["id"],
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount"),
                    "likeCount": statistics.get("likeCount"),
                    "commentCount": statistics.get("commentCount"),
                }

                extracted_list.append(video_data)
        return extracted_list        
    
    except requests.exceptions.RequestException as e:
        raise e
    
def load_data_as_csv(extracted_list):

    df = pd.DataFrame(extracted_list)
    output_dir = "/opt/spark-apps/youtube-video-stats/landing-input"

    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(output_dir, f"youtube_data_{date.today()}.csv")

    df.to_csv(file_path, index=False, encoding="utf-8", lineterminator="\n")

    print(f"Wrote CSV to {file_path}")

@dag(
    dag_id="youtube_vide_stats_manual",
    description="Run video-stats pipeline using existing functions, manual trigger.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["youtube", "api", "csv"]
)
def youtube_video_stats_manual():
    @task
    def t_get_playlist_id():
        return get_playlist_id()
    
    @task
    def t_get_video_id(playlist_id: str):
        return get_video_id(playlist_id) # returns list of video_ids (XCom)
    
    @task
    def t_extract_video_data(video_id_list):
        return extract_video_data(video_id_list)  # returns extracted_list (XCom)
    
    @task
    def t_load_data_as_csv(extracted_list):
        load_data_as_csv(extracted_list)
        return "done"
    
    pid = t_get_playlist_id()
    vids = t_get_video_id(pid)
    extracted = t_extract_video_data(vids)
    t_load_data_as_csv(extracted)

youtube_video_stats_manual()