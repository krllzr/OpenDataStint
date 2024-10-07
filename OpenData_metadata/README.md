## OpenData Metadata Fetching and Management

This repository is designed to automate the process of fetching and processing RKI's OpenData metadata from GitHub repositories and Zenodo datasets. The scripts run regularly (via GitHub Actions) and store the data as JSON files for further analysis.

### GitHub Metadata Fetching
Script: fetch_github_metadata.py
Purpose: Fetches non-traffic and traffic-related metadata for a hard-coded list of repositories (currently 34) under the specified GitHub organization/user ("robert-koch-institut").
Data Fetched:
Repository statistics: stargazers, watchers, forks, issues, size, subscribers.
Traffic data: views, unique views, clones, unique clones (if GitHub token is provided in the metadata_fetch.yml file in /.github/workflows).
As the traffic data covers only the last 14 days, the script also separately calculates differences between two consecutive days.

Data Storage:
A daily backup file is stored in metadata_github/ with the naming convention {timestamp}_github_opendata_metadata.json.
A cumulative file daily_github_opendata_metadata.json is updated with the latest metadata.

### Zenodo Metadata Fetching
Script: fetch_zenodo_metadata.py
Purpose: Fetches metadata for datasets in a specified Zenodo community.
Data Fetched:
Dataset views, unique views, unique downloads, versions, and GitHub associations.
Data Storage:
A daily backup file is stored in metadata_zenodo/ with the naming convention {timestamp}_zenodo_opendata_metadata.json.
A cumulative file daily_zenodo_opendata_metadata.json is updated with the latest metadata.

### Workflow Configuration
The GitHub Actions workflow runs daily at 5:00 AM (UTC) and can be manually triggered.
Both scripts are executed in the workflow, and the results are committed back to the repository.
