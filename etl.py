import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS","AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """ Create spark session to process dataframe/spark sql """
    
    spark = SparkSession \
        .builder \
        .appName("Climate Argiculture Spark application") \
        .getOrCreate()
    return spark


def process_climate_and_argiculture_data(spark, input_data, output_data):
    """ Extract song, log Data from s3, process them with Spark SQL, and save into another S3 bucket
        :param spark: Spark session
        :param input_data: S3 bucket url(name) contains song_data and log_data folder
        :param output_data: s3 bucket url(name) which will store the processed data
    """

    # read song data file
    csvfilepath = "s3a://ykng-bucket/climate_data/GlobalLandTemperaturesByCity.csv"  
    cf = spark.read.format("csv").option("header","true").load(csvfilepath)
    
     # read argiculture data file
    jsonfilepath = "s3a://ykng-bucket/climate_data/fao_data_crops_data.csv"
    jf = spark.read.format("csv").option("header","true").load(jsonfilepath)
        
    # create climate_data  and argiculture_data temp View
    cf.createOrReplaceTempView("climate_data")
    jf.createOrReplaceTempView("argiculture_data")

    # Tranform df to year, day, month in the climate table 
    staging_climate = spark.sql("""SELECT
                               Year(a.dt) as year, month(a.dt) as month, day(a.dt) as day,
                               AverageTemperature,AverageTemperatureUncertainty,
                               City, Country,
                               Latitude, Longitude
                               FROM climate_data a
                            """)
    #create new climate_data  temp View
    staging_climate.createOrReplaceTempView("climate_data2")

    # extract columns to create places table
    places_table = spark.sql("""
            SELECT DISTINCT 
            a.City, a.Country
            FROM climate_data a  
            """)
    
    # extract columns to create the element table
    element_table = spark.sql("""
    SELECT DISTINCT
        a.element_code, a.element
        FROM  argiculture_data a
    """)

    # extract columns to create the fact table
    climate_argi_table =  spark.sql("""
        SELECT DISTINCT
        md5(a.City || a.Country) climate_argiculture_id, 
        AverageTemperature, AverageTemperatureUncertainty, 
        Latitude, Longitude, element_code, 
        unit, value, value_footnotes, category,
        a.year, a.month , a.day
        FROM climate_data2 a  
        LEFT JOIN argiculture_data b
        ON a.year = b.year
        AND a.Country = b.country_or_area
        """)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://etl-song-data"
    
    process_climate_and_argiculture_data(spark, input_data, output_data)    

    spark.stop()


if __name__ == "__main__":
    main()