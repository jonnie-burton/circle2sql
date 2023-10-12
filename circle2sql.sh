#!/bin/#!/usr/bin/env bash

export PROJECT_ID=$(gcloud config get-value project)
export REGION=us-central1
export BUCKET=gs://${PROJECT_ID}-circle2sql
export PIPELINE_FOLDER=${BUCKET}
export RUNNER=DataflowRunner
export INPUT_PATH=${PIPELINE_FOLDER}
export LOCATIONS_TABLE_NAME=${PROJECT_ID}:circle2sql.locations
export EXITS_TABLE_NAME=${PROJECT_ID}:circle2sql.exits
export ELEMENTS_TABLE_NAME=${PROJECT_ID}:circle2sql.elements

python3 main.py \
--project=${PROJECT_ID} \
--region=${REGION} \
--staging_location=${PIPELINE_FOLDER}/staging \
--temp_location=${PIPELINE_FOLDER}/temp \
--runner=${RUNNER} \
--input_path=${INPUT_PATH} \
--locations_table_name=${LOCATIONS_TABLE_NAME} \
--exits_table_name=${EXITS_TABLE_NAME} \
--elements_table_name=${ELEMENTS_TABLE_NAME}
