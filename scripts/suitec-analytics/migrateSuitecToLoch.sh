#!/bin/bash

# Fail the entire script when one of the commands in it fails
set -e

echo_usage() {
  echo "SYNOPSIS"
  echo "     ${0} -d db_connection [-b s3_data_loch_bucket]"; echo
  echo "DESCRIPTION"
  echo "Available options"
  echo "     -d      Database connection information in the form 'host:port:database:username'. Required."
  echo "     -b      Data Loch bucket name where suitec data needs to be restored. Required."
}

while getopts "d:b:" arg; do
  case ${arg} in
    d)
      db_params=(${OPTARG//:/ })
      db_host=${db_params[0]}
      db_port=${db_params[1]}
      db_database=${db_params[2]}
      db_username=${db_params[3]}
      db_password=${db_params[4]}
      ;;
    b)
      s3_data_loch_bucket="${OPTARG}"
      ;;
  esac
done

echo ${db_params}

# Validation
[[ "${db_host}" && "${db_port}" && "${db_database}" && "${db_username}" ]] || {
  echo "[ERROR] You must specify complete suitec prod database connection information."; echo
  echo_usage
  exit 1
}
[[ "${s3_data_loch_bucket}" ]] || {
  echo "[ERROR] You must specify the s3 data-loch bucket where data will be synced"; echo
  echo_usage
  exit 1
}

if ! [[ "${db_password}" ]]; then
  echo -n "Enter database password: "
  read -s db_password; echo; echo
fi

# export password so that it need not be re-entered for every extracts
export PGPASSWORD=${db_password}

# clean up obsolete suitec data and recreate the folder structure to mimic S3 external table structures
rm -rf ~/suitec-data
mkdir -p ~/suitec-data

# declare an array containing all the suitec tables to be extracted into the data-loch
suitec_tables=( activities activity_types asset_users asset_whiteboard_elements assets assets_categories
canvas categories chats comments courses events pinned_user_assets users whiteboard_elements
whiteboard_members whiteboard_sessions whiteboards )

for i in "${suitec_tables[@]}"
do
  # create s3 directory structure locally and download suitec table extracts
  mkdir -p ~/suitec-data/$i
  echo "Downloading $i from suitec database."
  psql -h ${db_host} -U suitec -c "COPY (select * from $i) to STDOUT with NULL AS ''" > ~/suitec-data/$i/$i.tsv
done

# Copy the entire folder structure into the appropriate S3 location to restore
aws s3 cp --recursive ~/suitec-data/ s3://${s3_data_loch_bucket}/suitec-data/suitec-prod/public --sse
