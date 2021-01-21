# Data Warehouse 

This project aims to move processes and data of a music streaming startup, Sparkify, onto the AWS cloud. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in the app.

It builds an ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional and fact tables (i.e., star schema) optimized for queries on song play analysis.

## Getting Started

First step is to Launch a redshift cluster and create an IAM role that has read access to S3. The IaC.ipynb will help you to create these resourses step by step.

create_tables.py file creates all defined tables. etl.py file process the entire datasets in 2 steps: 1: Extracting all available and relevant data from S3 and stages them in Redshift cluster, 2: Transfering data based on defined conditions, and inserting prepaired data into the tables.

# Star Schema for Song Play Analysis

Fact Table

songplays - records in log data associated with song plays i.e. records with page NextSong

Dimension Tables
users - users in the app

songs - songs in music database

artists - artists in music database

time - timestamps of records in songplays broken down into specific units

## Running the tests

In order to test the ETL peocess, please run your queries in STEP:4 in Iac.ipynb file.



