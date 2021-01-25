# Scope the Project and Gather Data
The goal of this project to create a Extract Transform Load(ETL) data process on the global city climate change dataset from Kaggle. For data process, apache spark hosted in Amazon EMR cluster is used to organize data into STAR schema which consist of fact and dimension tables. 


#### The choice of tools and technologies for the project
    * The reasons Apache Spark is chosen:
        1.Apache spark supports both json/csv/other file formats with large amounts of data
        2.Apache Spark process large amount dataset faster due to its distributed cloud architecture.
        3.Spark provides both dataframe and SQL api to study data
    * The reason Amazon cloud is chosen as host for Apache Spark:
        1.Spark running on Amazon EMR cluster can utilize amazon s3 bucket directly with same credit
        2.Multiple Spark cluster can be created in Amazon cloud with scalabity. 

#### Propose how often the data should be updated and why.
    1. The Global Argiculture data is updated yearly.
    2. The Global Surface Temperature data is updated monthly.

# Describe and Gather Data  

#### I used two dataset in this project. 

1. FAOSTAT provides access to over 3 million time-series and cross sectional data relating to Most crop products under agricultural activity. Production Quantity and Seed is in tonnes and

2. Global Land and Ocean-and-Land Temperatures data. I have chosen GlobalLandTemperaturesByCity.csv which record temperature of all the city around the global. 

Both dataset has year recorded which can be used to connnect both dataset.

I believe there are relation between the climate change and the argiculture production.

##### Resources links:

[fao_data_crops_data.csv](https://www.kaggle.com/unitednations/global-food-agriculture-statistics) 2255349 rows

[city_temperature](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data) 8599212 rows



# Things I want to discover in this project
1. Change of temperatures of each country
2. The hottest city in the world 1995 and in 2015
3. The avarage temperature of 1995 and 2015

# Explore and Assess the Data and Cleaning Data


* Problem 1
There are missing value for avergaeTemperature and AverageTemperatureUncertainty on certain month of a city of a country. 

* Problem 2
Global-food-agriculture-statistics is recorded by year while GlobalLandTemperaturesByCity dataset is recorded by month.Therefore, I will separate dt into year, month, day so that I can use year column to combine both dataset.


* Problem 3
Most value_footnotes has null value and I think it is ok to drop this column


#  Define the Data Model

### Fact table and diemension table 

* Fact:

1. climate_fact
climate_id 	placeid	temperatures 	Latitude	Longitude element_code unit value value_footnotes category

***Most numerical value are fact and they cannot put in dimension table***
    
* Dimension: 
1. places
placeid  City	Country 	Region	State   country_or_area

2. material
element_code year element category

3. temperatures
temperatureid month AverageTemperature	AverageTemperatureUncertainty

3. date
Year   month	day

***Most nouns are dimension***

***Data Dictionary available in argiculture_climate.ipynb***
            
#### Approaches to problems under the following scenarios:
* The data was increased by 100x.
    1. Spark is built with scalabilty in mind and it could increase the number of nodes in our cluster in demand.
    2. The data populates a dashboard that must be updated on a daily basis by 7am every day.
    3. Since the project is done in amazon cloud, it utilize the Lamda function service to trigger a spark python process everyday to update the dashboard.

#### The database needed to be accessed by 100+ people.
    1. The dataset can be migrate to Redshift which provides cheap and faster than other data wirehouse due to its abitlity to utilize Amazon EMR, Sagemaker, Athena, and S3.