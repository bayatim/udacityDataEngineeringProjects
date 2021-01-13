# Data Pipelines with Airflow
In this project we introduce more automation and monitoring to sparkify data warehouse ETL pipelines by utilizing an open source apache project called Apache Airflow.

We create data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. We run tests against the datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## tasks dependencies
The dag follows the data flow as shown below.
![](https://github.com/bayatim/udacityDataEngineeringProjects/blob/main/Data_Pipelines/static/img/tasks_dependencies.png)

## Set up airflow server with docker
Plenty of tutorials available for setting up the airflow server. We use docker for this purpose. I found [this link](https://github.com/puckel/docker-airflow) usefull.