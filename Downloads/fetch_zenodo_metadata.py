import os
import requests
import pandas as pd
from datetime import datetime
from dateutil import tz

community_id = 'fc326bb3-f61b-4765-a778-f45a7a936483'

def fetch_zenodo_data(community_id):
    base_url = "https://zenodo.org/api/records?q=parent.communities.default%3A"
    local_timezone = tz.tzlocal()
    current_time = datetime.now(local_timezone).strftime('%Y-%m-%dT%H:%M:%S%z')
    records_data = []

    url = f"{base_url}{community_id}&size=100"
    while url:
        response = requests.get(url)
        data = response.json()

        if response.status_code != 200:
            print("Fehler beim Abrufen der Daten:", response.status_code)
            break

        for record in data['hits']['hits']:
            dataset_id = record['conceptrecid']
            views = record['stats']['views']
            unique_views = record['stats']['unique_views']
            downloads = record['stats']['downloads']
            unique_downloads = record['stats']['unique_downloads']
            versions = record['metadata']['relations']['version'][0]['index'] + 1
            title = record['metadata']['title']
            
            # Initialize github_name to 'none' by default
            github_name = 'none'
            
            related_identifiers = record.get('metadata', {}).get('related_identifiers', [])
            if related_identifiers:
                for item in related_identifiers:
                    if item.get("relation") == "isSupplementTo":
                        github_name = item.get("identifier").split('/')[-1]
                        break

            records_data.append({
                'Title': title,
                'Dataset ID': dataset_id,
                'Timestemp': current_time,
                'Versions': versions,
                'Views': views,
                'Unique Views': unique_views,
                'Downloads': downloads,
                'Unique Downloads': unique_downloads,
                'Github Name': github_name
            })

        # Check for next page link and update URL
        next_link = data['links'].get('next', None)
        url = next_link

    df = pd.DataFrame(records_data)
    return df

df = fetch_zenodo_data(community_id)

current_time = datetime.now().strftime('%Y-%m-%dT%H%M%S')

script_dir = os.path.dirname(os.path.abspath(__file__))
directory = os.path.join(script_dir, 'data')

os.makedirs(directory, exist_ok=True)

file_path_backup = os.path.join(directory, f'{current_time}_zenodo_community.json')
file_path_latest = os.path.join(directory, 'cumulative_zenodo_community_data.json')

df.to_json(file_path_backup, orient='records', lines=True)

if os.path.exists(file_path_latest):
    existing_data = pd.read_json(file_path_latest, orient='records', lines=True)
    updated_data = pd.concat([existing_data, df], ignore_index=True)
else:
    updated_data = df

updated_data.to_json(file_path_latest, orient='records', lines=True)
