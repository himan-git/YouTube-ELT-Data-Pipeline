import requests
import json
from datetime import date
from airflow.decorators import task
from airflow.models import Variable

#import os
#from dotenv import load_dotenv
#load_dotenv(dotenv_path="./.env")

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResults = 50


url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

@task
def getPlaylistID():
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        playlistID = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        return playlistID
    except requests.exceptions.RequestException as e:
        raise e
    
@task
def getVideoID(playlistId):
    temp = maxResults
    video_ids = []
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}"
    pageToken = None
    try:
        while temp!=0:
            url = base_url
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get('items', []):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)
                temp-=1
        return video_ids

    except requests.exceptions.RequestException as e:
        raise e
    
@task
def getVideoData(video_ids):
    extracted_data = []
    n = len(video_ids)
    video_ids_str = ''
    for i in video_ids:
        video_ids_str = ",".join(video_ids)
    url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=id&part=statistics&id={video_ids_str}&key={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    for item in data.get("items", []):
        video_id = item["id"]
        snippet = item["snippet"]
        contentDetails = item["contentDetails"]
        statistics = item["statistics"]

        video_data = {
            "video_id" : video_id,
            "title" : snippet["title"],
            "publishedAt" : snippet["publishedAt"],
            "duration" : contentDetails["duration"],
            "viewCount" : statistics.get("viewCount", None),
            "commentCount" : statistics.get("commentCount", None),
            "likeCount" : statistics.get("likeCount", None)
        } 
        extracted_data.append(video_data)
    
    return extracted_data

@task
def saveData(video_data):
    file_path = f"./data/YT_data_{date.today()}.json"

    with open(file_path, 'w', encoding='utf-8') as json_outfit:
        json.dump(video_data, json_outfit, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    playlistId = getPlaylistID()
    video_ids = getVideoID(playlistId)
    video_data = getVideoData(video_ids)
    saveData(video_data)
