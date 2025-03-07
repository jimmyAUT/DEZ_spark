#!/bin/bash

set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020
MONTH=$3

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"


URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${MONTH}.parquet"

LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${MONTH}"
LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${MONTH}.parquet"
LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

echo "downloading ${URL} to ${LOCAL_PATH}"
mkdir -p ${LOCAL_PREFIX}
wget ${URL} -O ${LOCAL_PATH}

done