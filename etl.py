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

def load_data_into_s3(places_dim, element_dim, fact_table):
    """ Here I save the transformed data into s3 in json format
        :param places_dim: places dimension table
        :param element_dim: element dimension table
        :param fact_table: the fact table contain id of places table, element table and other values
    """
    places_dim.createOrReplaceTempView("places_table")
    element_dim.createOrReplaceTempView("elements_table")
    fact_table.createOrReplaceTempView("fact_table")
    
    etl_path = "s3a://ykng-bucket/etl"
    places_dim.save(etl_path +'/places.json')
    element_dim.write.format('json').save(etl_path +'/elements.json')
    fact_table.write.partitionBy("year").format('json').save(etl_path +'/climate_argiculture/')

def unit_check(spark, query):
    test = spark.sql()
    test.show()
    print("unit check finished")

def data_quality_check(places_dim, element_dim, fact_table):
    """ Source/Count checks to ensure completeness
        :param places_dim: places dimension table
        :param element_dim: element dimension table
        :param fact_table: the fact table contain id of places table, element table and other values
    """
    print("Unit test Done")

    #Count the tables  predicted outcome is ??
    places_dim.show(100, False) 
    places_dim.count()

    element_dim.show(100, False) 
    element_dim.count()

    fact_table.show(100, False) 
    fact_table.count()


    print("Quality Check Done")

def process_climate_and_argiculture_data(spark, input_data, output_data):
    """ Extract song, log Data from s3, process them with Spark SQL, and save into another S3 bucket
        :param spark: Spark session
        :param input_data: S3 bucket url(name) contains song_data and log_data folder
        :param output_data: s3 bucket url(name) which will store the processed data
    """
    # Register helper function that convert country to region
    spark.udf.register("check_region", lambda x: region_fun(x))

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
    # unit test for element table
    places_table.createOrReplaceTempView("places_table")
    query = "SELECT DISTINCT * FROM places_table WHERE Country='United States'"
    unit_check(spark, query)
    
    # extract columns to create the element table
    element_table = spark.sql("""
    SELECT DISTINCT
        a.element_code, a.element
        FROM  argiculture_data a
    """)

    # unit test for element table
    element_table.createOrReplaceTempView("elements_table")
    query = "SELECT DISTINCT * FROM elements_table WHERE element_code = 41 "
    unit_check(spark, query)

    # extract columns to create the fact table
    climate_argi_table =  spark.sql("""
        SELECT DISTINCT
        md5(a.City || a.Country) climate_argiculture_id, 
        City,AverageTemperature, AverageTemperatureUncertainty, 
        Latitude, Longitude, element_code, 
        unit, value, value_footnotes, category,
        a.year, a.month , a.day
        FROM climate_data2 a  
        LEFT JOIN argiculture_data b
        ON a.year = b.year
        AND a.Country = b.country_or_area
        """)

    # unit test for fact table
    climate_argi_table.createOrReplaceTempView("fact_table")
    query = """SELECT DISTINCT 
            City,Country,year,month, day,AverageTemperature,
            AverageTemperatureUncertainty, value 
            FROM fact_table 
            JOIN places_table b ON a.City = b.City 
            WHERE year = 1995 AND  Country='United States'
            """
    unit_check(spark, query)

    data_quality_check(places_table, element_table, climate_argi_table)

def region_fun(x):
    if(x == 'United States' or x == 'Mexico' or x == 'Canada' or x == 'Guatemala' or x == 'Cuba' or x == 'Haiti' or
      x== 'Dominican Republic'):
        return  'Americas +'
    elif(x == 'Kazakhstan' or x == 'Kyrgyzstan' or x == 'Tajikistan' or x == 'Turkmenistan' or x == 'Uzbekistan'):
        return 'Central Asia +'
    elif(x == 'Estonia' or x == 'Latvia' or x == 'Lithuania' or x == 'Denmark' or x == 'Finland' or x == 'Iceland'
         or x == 'Norway' or x == 'Swede' ):
        return 'Northern Europe +'
    elif(x == 'Benin' or  x == 'Burkina Faso' or x == 'Cape Verde' or x == 'The Gambia'
         or x == 'Ghana' or x == 'Guinea' or x == 'Guinea-Bissau' or x == 'Ivory Coast'
         or x == 'Liberia' or x == 'Mali' or x == 'Mauritania' or x == 'Niger'
         or x == 'Nigeria' or x == 'Senegal' or x == 'Sierra Leone and Togo' or x == 'Saint Helena'
         or x == 'Ascension' or x == 'Tristan da Cunha'):
        return 'Western Africa +'
    elif(x == 'Afghanistan' or x == 'Pakistan' or x == 'India' or  x == 'Nepal'
         or x == 'Bhutan' or x == 'Bangladesh' or x == 'the Maldives' or x == 'Sri Lanka'):
        return 'Southern Asia +'
    elif(x == 'Tanzania' or  x == 'Kenya' or x == 'Uganda' or x == 'Rwanda' or x == 'Burundi' or x == 'South Sudan'
         or  x == 'Djibouti' or x == 'Eritrea' or x == 'Ethiopia' or x == 'Somalia' or x == 'Somaliland' 
         or x == 'Mozambique' or x == 'Madagascar' or x == 'Malawi' or x == 'Zambia' or x == 'Zimbabwe' or x == 'sudan'):
        return 'Eastern Africa +'
    elif(x == 'Armenia' or x == 'Belarus' or x == 'Bulgaria' or x == 'Cyprus'
         or x == 'Georgia' or x == 'Greece' or  x == 'Moldova' or x == 'Montenegro' or 
         x == 'North Macedonia' or x == 'Romania' or x == 'Russia' or x == 'Serbia' or x == 'Ukraine'):
        return 'Eastern Europe'
    elif(x == 'Brunei' or x == 'Burma' or  x == 'Cambodia' or x == 'Timor-Leste'
         or x == 'Indonesia' or x == 'Laos' or x == 'Malaysia' 
         or x == 'Philippines' or x == 'Singapore' or x == 'Thailand' or x == 'Vietnam'):
        return 'Southern Asia +'
        
    return x


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://etl-song-data"
    
    process_climate_and_argiculture_data(spark, input_data, output_data)    
   

    spark.stop()
    

if __name__ == "__main__":
    main()
