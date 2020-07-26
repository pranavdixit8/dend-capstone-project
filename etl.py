import configparser
from datetime import datetime, timedelta
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp,to_date
from pyspark.sql.types import TimestampType, DateType, IntegerType, StringType, DoubleType



config = configparser.ConfigParser()
config.read('/home/pranav/Desktop/data/projects/dend-capstone-project/dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['DEFAULT']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['DEFAULT']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """
    Creates Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport().getOrCreate()
        
    return spark


def find_visa_category(category):

    """
    Converts integer value of visa category to string value

    """
    if category ==1:
        return "Business"
    elif category ==2:
        return "Pleasure"
    elif category ==3:
        return "Student"



def get_port_to_city_dict(file):

    """
    Creates the port to city dictionary

    """
    with open(file) as f:
      lines = f.readlines()
    port_to_city_dict = {}
    for line in lines[302:961]:
      port_to_city_dict[line.split("=")[0].strip().strip("'")] = line.split("=")[1].strip().strip("'").split(",")[0]
    return port_to_city_dict


def get_city_to_port_dict(file):

    """
    Creates the port to city dictionary

    """
    port_to_city_dict = get_port_to_city_dict(file)
    city_to_port_dict = {v:k for k,v in port_to_city_dict.items()}
    return city_to_port_dict


def convert_to_date(num):

    """
    convert number of days past 2016,1,1 to date format
    """
    try:
        start_date = datetime(1960,1,1)
        
        return start_date + timedelta(days=int(num))
    except:
        return None



def duration_in_days(startdate, enddate):

    """
    calculates number of days between 2 dates

    """
    try:
        delta = enddate-startdate
        return delta.days
    except:
        return None


def process_immigration_data(spark, input_data, output_data, port_to_city_dict):
    
    """
    preproces sas7bdat files on immigration data

    Args:
        spark: Spark session object
        input_data: location of input folder
        output_data: location of output folder

    """

    # get filepath to immigration data file
    # immigration_data = os.path.join(input_data,'18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    # read immigration data file into dataframe
    # df =spark.read.format('com.github.saurfang.sas.spark').load(immigration_data, forceLowercaseNames=True, inferLong=True)

    # read immigration data file into dataframe
    df=spark.read.parquet(input_data)


    # extract columns to create immigration events table
    df = df.select(["cicid", "i94port","i94yr","i94mon","arrdate", "depdate", "i94visa", "gender", "i94bir"])


    # populate text category for visa category
    udf_find_visa_category = udf(lambda x: find_visa_category(x))
    df = df.withColumn("i94visa", udf_find_visa_category(df.i94visa))

    #convert arrival and depature date to date
    udf_convert_to_date = udf(lambda x : convert_to_date(x), DateType())
    df = df.withColumn("arrdate", udf_convert_to_date(df.arrdate))\
                    .withColumn("depdate", udf_convert_to_date(df.depdate))

    #calculate duration of immigration               
    udf_duration_in_days  = udf(lambda x,y : duration_in_days(x,y))
    df= df.withColumn("duration", udf_duration_in_days(df.arrdate, df.depdate))
    
    #change column type and name
    df = df.withColumn("cicid", df.cicid.cast(IntegerType()))\
                   .withColumn("i94yr", df.i94yr.cast(IntegerType()))\
                   .withColumn("i94mon", df.i94mon.cast(IntegerType()))\
                   .withColumn("i94bir", df.i94bir.cast(IntegerType()))\
                   .withColumnRenamed("i94yr", "year")\
                   .withColumnRenamed("i94mon", "month")\
                   .withColumnRenamed("arrdate", "arrival")\
                   .withColumnRenamed("depdate", "departure")\
                   .withColumnRenamed("i94bir", "age")\
                   .withColumnRenamed("i94visa", "visaCategory")

    #filter ports              
    df = df.filter(df.i94port.isin(list(port_to_city_dict.keys())))


    df.show(20)


    
    # write immigration events table to json files partitioned by year and months
    
    df.write.partitionBy("year", "month").mode("overwrite").json(os.path.join(output_data,"immigration_events"))

   


def process_port_data(spark, input_data, output_data, city_to_port_dict):
    
    """
    preproces csv files on port data

    Args:
        spark: Spark session object
        input_data: location of input folder
        output_data: location of output folder

    """
 

    # read port data file
    df = spark.read.csv(input_data, header=True)

    #add column i94port for port correspnding to the city Column
    city_port = udf(lambda city : city_to_port_dict[city.upper()] if city.upper() in city_to_port_dict else None)
    df = df.withColumn("i94port", city_port(df.City))


    #change column type and name
    df = df.withColumn("dt", to_date(df.dt))\
                             .withColumn("AverageTemperature",df.AverageTemperature.cast(DoubleType()))\
                             .withColumn("AverageTemperatureUncertainty", df.AverageTemperatureUncertainty.cast(DoubleType()))\
                             .select(["i94port","dt", "City", "Country", "Latitude", "Longitude", "AverageTemperature","AverageTemperatureUncertainty" ])

    #filter columns                         
    df= df.filter(df.i94port != 'null')\
                             .filter(df.AverageTemperature != 0)

    #take latest information for the city,port                        
    df.createOrReplaceTempView("temp")
    df = spark.sql(

    """
    SELECT t1.i94port, t1.dt, t1.City, t1.Country, t1.Latitude, t1.Longitude, t1.AverageTemperature, t1.AverageTemperatureUncertainty
    FROM temp t1
    JOIN (SELECT City, max(dt) as date
            from temp 
            group by City) t2
    ON
    t1.City = t2.City and t1.dt = t2.date
    ORDER by t1.City


    """)

    # write ports dataframe to json files partitioned by year and month
    df.write.mode("overwrite").json(os.path.join(output_data,"ports"))

  


def main():

    spark = create_spark_session()

    print(os.getcwd())
  
    input_immigration_data = os.path.join(os.path.dirname(os.path.realpath(__file__)), "sas_data")
    input_port_data = os.path.join(os.path.dirname(os.path.realpath(__file__)), "GlobalLandTemperaturesByCity.csv")
    input_labels_data = os.path.join(os.path.dirname(os.path.realpath(__file__)), "I94_SAS_Labels_Descriptions.SAS")
    output_data = "s3a://dend-2020-capstone/"
    
  
    port_to_city_dict = get_port_to_city_dict(input_labels_data)
    city_to_port_dict = get_city_to_port_dict(input_labels_data)

    
    process_immigration_data(spark, input_immigration_data, output_data, port_to_city_dict)    
    process_port_data(spark, input_port_data, output_data, city_to_port_dict)


if __name__ == "__main__":
    main()
