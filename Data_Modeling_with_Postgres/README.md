# Data Modeling with Postgres

This project aims to analyze Sparkify dataset on songs and user activity on a music streaming app, create a database schema and build an ETL pipeline using Python. The main focus is to understand what songs users are listening to.
Raw data resides in a directory of JSON logs on user activity, as well as a directory with JSON metadata on the songs.

## Getting Started

First step is to define and create fact and dimension tables for a star schema, and then write an ETL pipeline that transfers data from files in two directories into these tables in Postgres using Python and SQL.
 
create_tables.py file creates all defined tables. etl.py file process the entire datasets in 3 steps: 1: Extracting all available and relevant data 2: Transfering data based on defined conditions, and 3: inserting prepaired data into the tables.

# Star Schema for Song Play Analysis

Fact Table

songplays - records in log data associated with song plays i.e. records with page NextSong

Dimension Tables
users - users in the app

songs - songs in music database

artists - artists in music database

time - timestamps of records in songplays broken down into specific units

### Prerequisites

You need to install PostgreSQL (https://www.postgresql.org/download/) and Python 3 (https://www.python.org/downloads/). For installing packages on python one may use pip (https://pip.pypa.io/en/stable/installing/). The only required python package is pandas "pip install pandas".

How to run the project in terminal:
-> python create_tables.py 
-> python etl.py

## Running the tests

In order to test the ETL peocess, please run test.ipynp cells to see the content of each table. The songplays fact table should only have 1 row with values for value containing ID for both songid and artistid. The rest of the rows will have NONE values for those two variables.



