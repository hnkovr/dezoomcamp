echo "# generated here: https://chat.openai.com/chat/7215fc39-34aa-4786-a422-b417e8b10093"
echo "# with next prompt: Write well-dockumented (keeping most of instructions and with adding detailed comments) sh-script to automate doing this with input args in linux bash style as "--arg1 arg1_value" for all params and asking user input for choosing some options: <and here link to"
#https://github.com/hnkovr/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/README.md ==> https://raw.githubusercontent.com/hnkovr/data-engineering-zoomcamp/main/week_4_analytics_engineering/README.md
#>

#!/bin/bash

#set -x

echo "# Show input args"
echo "# args: $1 $2 $3 $4 $5 $6 $7 $8"

echo "# Define usage message"
usage() {
  echo "Usage: $0 --dir-name <directory-name> --dataset <bigquery-dataset> --project-id <gcp-project-id> [--region <region>] [--service-account <service-account-email>] [--credentials-path <path-to-credentials-json>] [--profile-name <profile-name>]"
  echo ""
  echo "Options:"
  echo "  -h, --help                   Display this usage message and exit"
  echo "  -d, --dir-name <directory-name>       Directory name for the project"
  echo "  -s, --dataset <bigquery-dataset>     BigQuery dataset name"
  echo "  -p, --project-id <gcp-project-id>   GCP project ID"
  echo "  -r, --region <region>          GCP region for BigQuery dataset. Default is EU"
  echo "  -a, --service-account <service-account-email> Service account email. By default, oauth will be used"
  echo "  -c, --credentials-path <path-to-credentials-json> Path to the credentials file. By default, it will be ~/.google/credentials/google_credentials.json"
  echo "  -n, --profile-name <profile-name>  Name of the profile in the profiles.yml file. Default is 'bq-dbt-workshop'"
  exit 1
}

echo "# Set defaults for optional parameters"
region="EU"
service_account=""
credentials_path="~/.google/credentials/google_credentials.json"
profile_name="bq-dbt-workshop"

echo "# Parse command line arguments"
while [ "$#" -gt 0 ]; do
  case "$1" in
    -h|--help)
      usage
      ;;
    -d|--dir-name)
      dir_name="$2"
      shift 2
      ;;
    -s|--dataset)
      dataset="$2"
      shift 2
      ;;
    -p|--project-id)
      project_id="$2"
      shift 2
      ;;
    -r|--region)
      region="$2"
      shift 2
      ;;
    -a|--service-account)
      service_account="$2"
      shift 2
      ;;
    -c|--credentials-path)
      credentials_path="$2"
      shift 2
      ;;
    -n|--profile-name)
      profile_name="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done

echo "# Check if required parameters are provided"
if [[ -z "$dir_name" || -z "$dataset" || -z "$project_id" ]]; then
  echo "Missing required parameters"
  usage
fi

echo "# Create directory and move into it"
mkdir "$dir_name"
cd "$dir_name"

echo "# Copy the Dockerfile to the current directory"
cp <path-to-Dockerfile> .

echo "# Create docker-compose.yaml file"
cat <<EOF > docker-compose.yaml
version: '3'
services:
  dbt-bq-dtc:
    build:
      context: .
      target: dbt-bigquery
    image: dbt/bigquery
    volumes:
      - .:/usr/app
      - ~/.dbt/:/root/.dbt/
      - $credentials_path:/.google/credentials/google_credentials.json
    network_mode: host
EOF

echo "# Create profiles.yml in the ~/.dbt/ directory or append to an existing file"
cat <<EOF > ~/.dbt/profiles.yml
# Example:
#bq-dbt-workshop:
#   outputs:"
#     dev:"
#       dataset: <bigquery-dataset>"
#       fixed_retries: 1"
#       keyfile: /.google/credentials/google_credentials.json"
#       location: EU"
#       method: service-account"
#       priority: interactive"
#       project: <gcp-project-id>"
#       threads: 4"
#       timeout_seconds: 300"
#       type: bigquery"
#   target: dev"

$profile_name:
  outputs:
    dev:
      dataset: $dataset
      fixed_retries: $fixed_retries
      keyfile: /.google/credentials/google_credentials.json
      location: $location
      method: service-account
      priority: interactive
      project: $project
      threads: $threads
      timeout_seconds: $timeout_seconds
      type: bigquery
  target: dev
EOF

echo "# Build and initialize dbt project"
docker compose build
docker compose run dbt-bq-dtc init

echo "# Prompt user for project name"
#if $project_name
read -p "Enter project name: " project_name

echo "# Replace project name in dbt_project.yml"
sed -i "s|name: taxi_rides_ny|name: $project_name|" dbt/taxi_rides_ny/dbt_project.yml

echo "# Replace profile name in dbt_project.yml"
sed -i "s|profile: 'taxi_rides_ny'|profile: '$profile_name'|" dbt/taxi_rides_ny/dbt_project.yml

echo "# Testing connection..."
docker compose run --workdir="//usr/app/dbt/taxi_rides_ny" dbt-bq-dtc debug

echo "Done!"

set +x