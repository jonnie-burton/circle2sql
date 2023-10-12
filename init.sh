#!/bin/#!/usr/bin/env bash

sudo apt-get update && sudo apt-get install -y python3-venv
# Create and activate virtual environment
python3 -m venv df-env
source df-env/bin/activate
python3 -m pip install -q --upgrade pip setuptools wheel
python3 -m pip install apache-beam[gcp]
gcloud services enable dataflow.googleapis.com

echo "Creating bucket to store input .WLD files and BQ dataset to be the output sink"

PROJECT_ID=$(gcloud config get-value project)

# GCS bucket
gsutil mb -l US gs://${PROJECT_ID}-circle2sql

# BiqQuery Dataset
bq mk --location=US ${PROJECT_ID}:circle2sql
