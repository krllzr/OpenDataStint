import requests
from datetime import datetime
import json
import os
import pandas as pd
import numpy as np

api_url = "https://api.github.com"

user = "robert-koch-institut"

# Get the access token from 'metadata_fetch.yml' or set it to 'None' if it fails
GITHUB_TOKEN = os.getenv('TOKEN', None)

repositories = [
    "Abwassersurveillance_AMELAG",
    "AKTIN_Daten_zur_Aufenthaltsdauer_in_Notaufnahmen",
    "Appendix_Potential_COVID-19_test_fraud_detection",
    "ARE-Konsultationsinzidenz",
    "Bundesweiter_klinischer_Krebsregisterdatensatz-Datenschema_und_Klassifikationen",
    "BURDEN_2020_-_Krankheitslast_in_Deutschland_und_seinen_Regionen",
    "Carotid_intima-media_thickness-Reference_percentiles_from_the_KiGGS_study",
    "Corona-Datenspende_Teildatensatz_Vitaldaten",
    "Corona-Datenspende_Teildatensatz_Erleben_und_Verhalten_in_der_Pandemie",
    "COVID-19-Hospitalisierungen_in_Deutschland",
    "COVID-19-Impfungen_in_Deutschland",
    "COVID-19-Todesfaelle_in_Deutschland",
    "COVID-19_7-Tage-Inzidenz_in_Deutschland",
    "COVID-ARE-Konsultationsinzidenz",
    "COVID-SARI-Hospitalisierungsinzidenz",
    "Daten_der_Notaufnahmesurveillance",
    "epylabel",
    "ESI-CorA_SARS-CoV-2-Abwassersurveillance",
    "German_Index_of_Socioeconomic_Deprivation_GISD",
    "Gesundheit_in_Deutschland_Aktuell",
    "GrippeWeb_Daten_des_Wochenberichts",
    "Hochfrequente_Mental_Health_Surveillance",
    "Inanspruchnahme_von_Routineimpfungen_in_Deutschland-Ergebnisse_aus_der_KV-Impfsurveillance",
    "Influenzafaelle_in_Deutschland",
    "Intensivkapazitaeten_und_COVID-19-Intensivbettenbelegung_in_Deutschland",
    "Observatorium_serologischer_Studien_zu_SARS-CoV-2_in_Deutschland",
    "Polioviren_im_Abwasser-PIA",
    "Respiratorische_Synzytialvirusfaelle_in_Deutschland",
    "SARI-Hospitalisierungsinzidenz",
    "SARS-CoV-2-Infektionen_in_Deutschland",
    "SARS-CoV-2-Nowcasting_und_-R-Schaetzung",
    "SARS-CoV-2-PCR-Testungen_in_Deutschland",
    "SARS-CoV-2-Sequenzdaten_aus_Deutschland",
    "StopptCOVID-Studie_Daten_Analyse_und_Ergebnisse"
]

current_datetime = datetime.now().strftime('%Y-%m-%dT%H%M%S')
timestamp = datetime.now().isoformat()

# Get nontraffic repository metadata
def get_general_repo_data(repository_full_name, api_url):
    repo_url = f"{api_url}/repos/{repository_full_name}"

    response = requests.get(repo_url)

    if response.status_code == 200:
        repo_data = response.json()
        metadata = {
            "repository": repository_full_name,
            "timestamp": timestamp,
            "stargazers_count": repo_data.get('stargazers_count', 0),
            "watchers_count": repo_data.get('watchers_count', 0),
            "forks_count": repo_data.get('forks_count', 0),
            "open_issues_count": repo_data.get('open_issues_count', 0),
            "size": repo_data.get('size', 0),
            "subscribers_count": repo_data.get('subscribers_count', 0)
        }
        return metadata
    else:
        print(f"Error fetching data for {repository_full_name}: {response.status_code}")
        return None

# Get traffic repository metadata
def get_traffic_data(repository_full_name, api_url, token):
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }

    traffic_data = {
        'views': None,
        'unique_views': None,
        'clones': None,
        'unique_clones': None
    }

    views_url = f"{api_url}/repos/{repository_full_name}/traffic/views"
    response = requests.get(views_url, headers=headers)

    if response.status_code == 200:
        views_data = response.json()
        traffic_data['views'] = views_data.get('count', None)
        traffic_data['unique_views'] = views_data.get('uniques', None)
    elif response.status_code == 204:
        print(f"No traffic data available for views for {repository_full_name}. Status Code: {response.status_code}")
    else:
        print(f"Error fetching views for {repository_full_name}: {response.status_code} - {response.text}")

    clones_url = f"{api_url}/repos/{repository_full_name}/traffic/clones"
    response = requests.get(clones_url, headers=headers)

    if response.status_code == 200:
        clones_data = response.json()
        traffic_data['clones'] = clones_data.get('count', None)
        traffic_data['unique_clones'] = clones_data.get('uniques', None)
    elif response.status_code == 204:
        print(f"No traffic data available for clones for {repository_full_name}. Status Code: {response.status_code}")
    else:
        print(f"Error fetching clones for {repository_full_name}: {response.status_code} - {response.text}")

    return traffic_data

all_repo_data = []

for repo in repositories:
    repository_full_name = f"{user}/{repo}"
    repo_metadata = get_general_repo_data(repository_full_name, api_url)

    if repo_metadata:
        if GITHUB_TOKEN:
            traffic = get_traffic_data(repository_full_name, api_url, GITHUB_TOKEN)
            repo_metadata.update(traffic)
        else:
            print("GITHUB_TOKEN is not set or empty. Skipping traffic data.")

            traffic = {
                'views': None,
                'unique_views': None,
                'clones': None,
                'unique_clones': None
            }
            repo_metadata.update(traffic)

        all_repo_data.append(repo_metadata)

df = pd.DataFrame(all_repo_data)

traffic_data_columns = ['views', 'unique_views', 'clones', 'unique_clones']
for col in traffic_data_columns:
    if col not in df.columns:
        df[col] = None

df['timestamp'] = pd.to_datetime(df['timestamp'])

script_dir = os.path.dirname(os.path.abspath(__file__))
directory = os.path.join(script_dir, 'metadata_github')

os.makedirs(directory, exist_ok=True)

file_path_backup = os.path.join(directory, f'{current_datetime}_github_opendata_metadata.json')
file_path_latest = os.path.join(directory, 'daily_github_opendata_metadata.json')

if os.path.exists(file_path_latest):
    existing_data = pd.read_json(file_path_latest, orient='records', lines=True)
    existing_data['timestamp'] = pd.to_datetime(existing_data['timestamp'])
else:
    existing_data = pd.DataFrame()

traffic_diff_columns = ['views_diff', 'unique_views_diff', 'clones_diff', 'unique_clones_diff']
traffic_columns = traffic_data_columns + traffic_diff_columns

for col in traffic_data_columns:
    if col not in existing_data.columns:
        existing_data[col] = None

if not existing_data.empty:
    last_data = existing_data.sort_values('timestamp').groupby('repository').tail(1)

    for col in traffic_data_columns:
        if col not in last_data.columns:
            last_data[col] = None

    df_with_diff = df.merge(
        last_data[['repository'] + traffic_data_columns],
        on='repository', how='left', suffixes=('', '_prev')
    )

    for col in traffic_data_columns:
        df_with_diff[f'{col}_diff'] = df_with_diff[col] - df_with_diff[f'{col}_prev']

    prev_columns = [f'{col}_prev' for col in traffic_data_columns]
    df_with_diff = df_with_diff.drop(columns=prev_columns)
else:
    df_with_diff = df.copy()
    for col in traffic_data_columns:
        df_with_diff[f'{col}_diff'] = None

for col in traffic_columns:
    if col not in df_with_diff.columns:
        df_with_diff[col] = None

desired_columns_order = [
    'repository', 'timestamp',
    'stargazers_count', 'watchers_count', 'forks_count', 'open_issues_count',
    'size', 'subscribers_count',
    'views', 'views_diff', 'unique_views', 'unique_views_diff',
    'clones', 'clones_diff', 'unique_clones', 'unique_clones_diff'
]

df_with_diff = df_with_diff[desired_columns_order]

df_with_diff.to_json(file_path_backup, orient='records', lines=True, date_format='iso')

updated_data = pd.concat([existing_data, df_with_diff], ignore_index=True)

for col in desired_columns_order:
    if col not in updated_data.columns:
        updated_data[col] = None

updated_data = updated_data[desired_columns_order]

updated_data.to_json(file_path_latest, orient='records', lines=True, date_format='iso')
