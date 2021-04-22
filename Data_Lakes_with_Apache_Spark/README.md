# Data Lake

This project aims to move the sparkify data warehouse to a data lake. Raw data resides in a S3 directory of JSON logs on user activity, as well as a directory with JSON metadata on the songs.

## Getting Started

First step is to build an ETL pipeline that extracts data from S3, processes them using spark (i.e., extracs fact and dimension tables from song and log data), and loads the data back into S3 as a set of dimentional tables.
 
etl.py file does all the above steps. Reader class creates a spark session, and loads data.  This class make sure to extract data one time as this is a very slow process.

# Schema for Song Play Analysis

Fact Table

songplays - records in log data associated with song plays i.e. records with page NextSong

Dimension Tables
users - users in the app

songs - songs in music database

artists - artists in music database

time - timestamps of records in songplays broken down into specific units

# output data
Each of the five tables are written to parquet files in a separate analytics directory on S3. Each table has its own folder within the directory. Songs table files are partitioned by year and then artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.

This new output tables allow the analytics team to continue finding insights in what songs their users are listening to.

### Prerequisites

You may reide and run the code on AWS EMR service (https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs-prerequisites.html). 

How to run the project in terminal:
-> python etl.py