# Project Title
## Data Engineering Capstone Project

### Project Summary
* The goal of this project is to evaluate the impact of weather's temperature on immagrants movements over April, 2016 in USA
* Apache Spark is used to extract and transform raw data, and make a datawarehouse in parquet file format. 
* The star schema is used to develop a database, which will be effectively used for handling analytical queries.

### Scope the Project and Gather Data

#### Scope 
* This project extracts raw data from two sources as described below. 
* It creates a datamodel of immagrants' movement in US consistsing of one fact tables referencing two dimension tables.

#### Describe and Gather Data

##### I94 Immigration Data: 
* This data comes from the [US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html).
* Dataset includes infoes on individual incomming immigrants and thei ports on entry. 

##### World Temperature Data:
* This dataset comes from [Kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
* Dataset includes infoe on temprature on cities globally. This project only uses data of US cities.

#### Mapping Out Data Pipelines
* Import all neccassery libraries and functions
* Initialize reader object and assign spark and data to appropriate variables
* Process immigration data to create clean immigration dimention table
    * filter, clean and load data
* Process temperature data to create clean temperature dimention table
    * filter, clean and load data
* Create fact table from dimention tables
    * join dimention tables and load data
* Perform data quality check
    * Count check to ensure completeness


#### Data dictionary
Included in utils/DataDictionary.md