# Capestone Project 

# **Data Dictionary**
## Inmigration Dimention Table
* dt: date (nullable = true) - date of arrival
* i194addr: 2 character code (nullable = true) - where the immigrants resides in USA,
* i94port: 3 character code (nullable = false) - destination USA city

## Temperature Dimension Table:
* dt: date (nullable = true) - date of recorded temperature
* AverageTemperature: decimal (nullable = true) - average recorded temperature per month
* City: string (nullable = true) - city name
* Country: string (nullable = true) - country name
* i94port: 3 character code (nullable = false) - entry port extracted from i94-immigration data
    
## Fact Table
* dt: date (nullable = false) - date of arrival,
* i94port: 3 character code (nullable = false) - entry port
* AverageTemperature: decimal (nullable = false) - average temperature of destination city
