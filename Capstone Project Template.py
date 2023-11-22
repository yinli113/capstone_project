#!/usr/bin/env python
# coding: utf-8

# ## Project Title: Data Engineering Capstone Project
# 
# ### This script is part of a Data Engineering Capstone Project aimed at creating
# a data warehouse with fact and dimension tables for analysis and business intelligence.**
# 
# #### The project encompasses the following steps:
# 1. Scope the Project and Gather Data
# 2. Explore and Assess the Data
# 3. Define the Data Model
# 4. Run ETL to Model the Data
# 5. Complete Project Write Up

# In[1]:


# Do all imports and installs here
import pandas as pd
import pyspark
from pyspark.sql import SparkSession, types as T
from pyspark.sql.types import DateType, StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, TimestampType
from pyspark.sql.functions import to_date, col, udf, unix_timestamp, year, month, lit, upper
from pyspark.sql.functions import count, when, isnan
from pyspark.sql.functions import monotonically_increasing_id, concat
from pyspark.sql.functions import col, year, month, date_format
from pyspark.sql.functions import monotonically_increasing_id
import os
import configparser
import logging
from pathlib import Path
from datetime import datetime, timedelta


# ### Step 1: Scope the Project and Gather Data
# 
# #### Scope 
# This project investigates I94 immigration data and US demographic data to create a data warehouse with Fact and Dimension tables for analysis and business intelligence.( We assume the data is stored in AWS s3 bucket, and will be written back in s3 bucket. )
# 
# Spark and Python are used to create the ETL pipeline , AWS S3 is uded to store the data and tables. The tables are designed to provide a comprehensive view of immigration events, combining individual immigration details with demographic information. The fact table is intended for analytical queries that need to explore immigration patterns, demographic trends, and the relationships between these elements(for example, the avergae population in certain year and certain state).
# 
# The original sources of the data are all from Udacity, below is the link:
# 1. [I94 Immigration Data](https://travel.trade.gov/research/reports/i94/historical/2016.html)
# 2. [Us Cities Demographics](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
# 
# #### Describe and Gather Data 
# ##### I94 Immigration Data:
# 1. This data comes from the US National Tourism and Trade Office. A data dictionary is included.[data_link_here](https://travel.trade.gov/research/reports/i94/historical/2016.html)
# 2. This data is a SAS file, There's a sample file in csv format. Data contains over 20 columns including country code, arriving date, age, state, visa type ect.
# 
# ##### U.S. City Demographic Data:
# 1. This data comes from OpenSoft. [data_link_here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
# 2. Thi data is a csv file. Data contains demographics of all US cities and with a population distribution and race number.

# **Immigration Data**

# In[2]:


# Read in the data here
df_immi=pd.read_csv('immigration_data_sample.csv')
df_immi.head()


# **US Cities Demographics Data**

# In[3]:


df_demo = pd.read_csv('us-cities-demographics.csv', delimiter=';')
df_demo.head(5)


# ### Step 2: Explore and Assess the Data
# 
# 

# #### Data Exploration:
# 
# - Start by working with a smaller subset of data, such as the immigration_data_sample.csv, to get a sense of its structure and contents.
# - Identify data quality issues, like missing values, duplicate data, etc.
# 
# 

# In[4]:


# Read in the data of immigration 
df_immi=pd.read_csv('immigration_data_sample.csv')
df_immi.head()


# In[5]:


df_immi.info()


# In[6]:


# Read in the data of demographics
df_demo = pd.read_csv('us-cities-demographics.csv', delimiter=';')
df_demo.head(5)


# In[7]:


df_demo.info()


# In[8]:


# selecting the columns that could be used to create dimension tables, and renaming the columns name for better understanding
stage_immi = df_immi[['cicid',  'i94cit', 'i94res', 'i94port',                       'arrdate','i94addr', 'depdate', 'i94visa',                      'occup', 'biryear',                      'gender', 'airline',  'fltno']]

stage_immi.columns = ['cic_id', 'country_code_1','country_code_2','city_code',                      'arrive_date','state_code', 'depart_date','visa_code',                      'occupation','birth_year',                      'gender','airline','flight_number']


stage_immi.head()


# #### Explore bigger data 
# 1. Identify data quality issues, like missing values, duplicate data, etc.
# 2. Document steps necessary to clean the data
# 

# SPARK SESSION

# In[9]:


#configure the saprk setting
config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


# In[10]:


# create a spark session
spark = SparkSession.builder.    config("spark.jars.repositories", "https://repos.spark-packages.org/").    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0").    enableHiveSupport().getOrCreate()


# In[11]:


# Read in immigration data 
immigration_data = spark.read.format('com.github.saurfang.sas.spark').            load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')

# Create a functoin to rename the columns' name
rename_columns = lambda table, new_columns: table.select(
    *[col(original).alias(new) for original, new in zip(table.columns, new_columns)])


# extract columns to create immigration staging table
stage_immigration = immigration_data.select('cicid',  'i94cit', 'i94res', 'i94port',                       'arrdate','i94addr', 'depdate', 'i94visa',                      'occup', 'biryear',                      'gender', 'airline',  'fltno').distinct()
                
    
# rename the columns
new_column_names = ['cic_id', 'country_code_1','country_code_2','city_code',                      'arrive_date','state_code', 'depart_date','visa_code',                      'occupation','birth_year',                      'gender','airline','flight_number']
    
stage_immigration = rename_columns(stage_immigration, new_column_names)
                                  
# Write stage_immigration table to parquet files partitioned by 'state_code'
try:
    stage_immigration.write.mode("overwrite").partitionBy("state_code")            .parquet(os.path.join("./final/", "stage_immigration"))
    print("stage_immigration write completed successfully")
except Exception as e:
    print(f"Error writing stage_immigration table: {str(e)}")


# In[12]:


stage_immigration.printSchema()


# In[13]:


# Read in US demographics data
stage_demographics=spark.read.csv('us-cities-demographics.csv', inferSchema=True, header=True, sep=';')

# extract columns to create demographic staging table
stage_demographics = stage_demographics.select('City', 'State', 'Median Age', 'Male Population', 'Female Population',
       'Total Population', 'Number of Veterans', 'Foreign-born',
       'Average Household Size', 'State Code', 'Race', 'Count').distinct()
                    
# rename the columns
new_column_names = ['City', 'State', 'Median_Age', 'Male_Population', 'Female_Population',
       'Total_Population', 'Number_of_Veterans', 'Foreign_born',
       'Average_Household_Size', 'State_Code', 'Race', 'Count']
    
stage_demographics = rename_columns(stage_demographics, new_column_names)


# Write stage_demographics table to parquet files partitioned by 'state_code'
try:
    stage_demographics.write.mode("overwrite").partitionBy("state_code")            .parquet(os.path.join("./final/", "stage_demographics"))
    print("stage_demographics write completed successfully")
except Exception as e:
    print(f"Error writing stage_demographics table: {str(e)}")


# In[14]:


stage_demographics.toPandas().head()


# Cleaning NULL values that could cause skewness of distribution

# In[15]:


# Finding the null percentage in each column of stage_immigration

test_immigration = stage_immigration.select([
    (count(when(isnan(c) | col(c).isNull(), c)) * 100 / stage_immigration.count()).alias(c)
    for c in stage_immigration.columns
])

test_immigration.show()


# In[16]:


# occupation's null percentage is too high , need to drop:
stage_immigration.drop('occupation')


# In[17]:


# Finding the null percentage in each column of stage_demographics
test_demographics = stage_demographics.select([
    (count(when(isnan(c) | col(c).isNull(), c))*100/stage_demographics.count()).alias(c)
    for c in stage_demographics.columns
])

test_demographics.show()
# Result shows the percentage of null value is low and even in each column of table stage_demographics 


# ### Step 3: Define the Data Model
# 

# #### 3.1 Conceptual Data Model
# Map out the conceptual data model and explain why you chose that model
# 

# **As Star Schema can map multidimensional data structures in relational databases and is used primarily in Data Warehouses and OLAP apps and for BI, I will create a fact table as the center, various dimension tables are grouped around, creating the whole star schema.**
# 
# ### Fact Table
# 
# #### immi_demographics
# - cic_id: double
# - country_code_1
# - country_code_2
# - city_code
# - arrive_date
# - immigration_state_code
# - depart_date
# - visa_code
# - birth_year
# - gender
# - airline
# - flight_number
# - avg_Median_Age
# - avg_Male_Population
# - avg_Female_Population
# - avg_Total_Population
# - avg_Number_of_Veterans
# - avg_Foreign_born
# - demographics_state_code
# - Race
# - year
# - month
# 
# ### Dimension Tables
# 
# #### 1. dim_immi_personel
# - cic_id
# - country_code_1
# - country_code_2
# - birth_year
# - gender
# 
# #### 2. dim_immi_airline
# - travel_id
# - city_code  
# - airline
# - flight_number
# - visa_code
# 
# #### 3. dim_arrive_calendar
# - arrive_date
# - year
# - month
# - week
# - day
# 
# #### 4. dim_depart_calendar
# - depart_date
# - year
# - month
# - week
# - day
# 
# #### 5. demo_population_dim
# - demo_pop_id
# - State
# - Median_Age
# - Male_Population
# - Female_Population
# - Total_Population
# - Foreign_born
# - State_Code
# - Race

# #### 3.2 Mapping Out Data Pipelines
# List the steps necessary to pipeline the data into the chosen data model

# #### The pipeline steps are described below:
# 
# ##### 1. Load raw datasources and form the staging tables: stage_immigration and stage_demographics.
# - '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat' contains I94 Immigration datasource.
# - 'us-cities-demographics.csv' contains US Cities Demographics datasource.
# 
# ##### 2. Describe DataFrame structure:
# -  Pandas dataframe for small datasource exploration purpose, for example, undstand the column name, data type,ect.
# - Spark dataframe for bigger datasource exploration.
# 
# ##### 3. Clean the dataframe:
# - rename the columns' name for better understanding.
# - drop the columns with high percentage null values.
# 
# ##### 4. Transform staging tables to fact and dimemsion tables and write Spark DataFrame to the Parquet file.
# - immi_demographics (fact_table)
# - dim_immi_personel
# - dim_immi_airline
# - dim_arrive_calendar
# - dim_depart_calendar
# - demo_population_dim
# 
# #### 5. Create data quality check for fact and dim tables.

# ### Step 4: Run Pipelines to Model the Data 
# #### 4.1 Create the data model
# Build the data pipelines to create the data model.

# In[18]:


dim_immi_personel = (
    stage_immigration
    .select('cic_id', 'country_code_1', 'country_code_2', 'birth_year', 'gender')
    .filter(col('cic_id').isNotNull())
    .dropDuplicates(['cic_id'])
)
# Write dim_immi_personel table to parquet files 
try:
    stage_demographics.write.mode("overwrite").parquet(os.path.join("./final/", "dim_immi_personel"))
    print("dim_immi_personel write completed successfully")
except Exception as e:
    print(f"Error writing dim_immi_personel table: {str(e)}")


# In[19]:


dim_immi_airline = stage_immigration.select(
        concat(monotonically_increasing_id(), col('city_code')).alias('travel_id'),
        col('city_code'),  
        col('airline'),
        col('flight_number'),
        col('visa_code')
    )
# Write dim_immi_airline table to parquet files 
try:
    stage_demographics.write.mode("overwrite").parquet(os.path.join("./final/", "dim_immi_airline"))
    print("dim_immi_airline write completed successfully")
except Exception as e:
    print(f"Error writing dim_immi_airline table: {str(e)}")


# In[ ]:


from datetime import datetime, timedelta

def dateConvert(date):
    """
    Convert an integer date to a datetime object.

    This function takes an integer representing a date as the number of days 
    from a fixed point in time (January 1, 1960) and converts it into a 
    datetime object. If the input is None, the function returns None.

    Args:
        date (int or None): The date represented as an integer or None.

    Returns:
        datetime or None: The converted date as a datetime object, 
                          or None if the input is None.
    """
    if date is not None:
        # Convert the integer date to a Timestamp using a UDF
        return datetime(1960, 1, 1) + timedelta(days=date)
    else:
        return None
    
# Register the dateConvert function as a UDF
dateConvertUDF = udf(dateConvert, TimestampType())

# Use the UDF to convert the arrive_date and depart_date columns
stage_immigration = stage_immigration.withColumn('arrive_date', dateConvertUDF(col('arrive_date')))
stage_immigration = stage_immigration.withColumn('depart_date', dateConvertUDF(col('depart_date')))


# In[21]:


# Extract year, month, week, and day from arrive_date using PySpark functions

# Remove duplicates from the original DataFrame
stage_immigration = stage_immigration.dropDuplicates(["arrive_date"])

# Extract year, month, week, and day from depart_date using PySpark functions
dim_arrive_calendar = stage_immigration.select(
    "arrive_date",
    year("arrive_date").alias("year"),
    month("arrive_date").alias("month"),
    date_format("arrive_date", "w").alias("week"),
    date_format("arrive_date", "d").alias("day")
)

# Writing the DataFrame to parquet format with partitioning
output_path = os.path.join("./final/", "dim_arrive_calendar")
try:
    dim_arrive_calendar.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
    print("dim_arrive_calendar write completed successfully")
except Exception as e:
    print(f"Error writing dim_arrive_calendar table: {str(e)}")


# In[22]:


# Extract year, month, week, and day from depart_date using PySpark functions

# Remove duplicates from the original DataFrame
stage_immigration = stage_immigration.dropDuplicates(["depart_date"])

# Extract year, month, week, and day from depart_date using PySpark functions
dim_depart_calendar = stage_immigration.select(
    "depart_date",
    year("depart_date").alias("year"),
    month("depart_date").alias("month"),
    date_format("depart_date", "w").alias("week"),
    date_format("depart_date", "d").alias("day")
)

# Writing the DataFrame to parquet format with partitioning
output_path = os.path.join("./final/", "dim_depart_calendar")
try:
    dim_depart_calendar.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
    print("dim_depart_calendar write completed successfully")
except Exception as e:
    print(f"Error writing dim_depart_calendar table: {str(e)}")


# In[23]:


demo_population_dim = (
    stage_demographics
    .select(
        monotonically_increasing_id().alias('demo_pop_id'),
        col('State'),
        col('Median_Age'),
        col('Male_Population'),
        col('Female_Population'),
        col('Total_Population'), 
        col('Foreign_born'),
        col('State_Code'),
        col('Race')
    )
    .dropDuplicates(['demo_pop_id'])
)

# Write demo_population_dim table to parquet files 
try:
    demo_population_dim.write.mode("overwrite").parquet(os.path.join("./final/", "demo_population_dim"))
    print("demo_population_dim write completed successfully")
except Exception as e:
    print(f"Error writing demo_population_dim table: {str(e)}")


# In[24]:


# Creating temporary tables 
stage_demographics.createOrReplaceTempView("temp_demographics")
stage_immigration.createOrReplaceTempView("temp_immigration")

'''
creating the fact table immi_demographics in the provided code is to consolidate
and combine relevant information from two source tables, temp_immigration and 
temp_demographics, into a single table that can be used for analytical purposes.
The code performs inner join between temp_immigration and an aggregated version
of temp_demographics. The join is based on the state_code column, connecting immigration
data with demographic data.

'''
immi_demographics = spark.sql("""
    SELECT 
        ti.cic_id, 
        ti.country_code_1,
        ti.country_code_2,
        ti.city_code,
        ti.arrive_date,
        ti.state_code AS immigration_state_code,
        ti.depart_date,
        ti.visa_code,
        ti.birth_year,
        ti.gender,
        ti.airline,
        ti.flight_number,
        AVG(td.Median_Age) as avg_Median_Age,
        AVG(td.Male_Population) as avg_Male_Population,
        AVG(td.Female_Population) as avg_Female_Population,
        AVG(td.Total_Population) as avg_Total_Population,
        AVG(td.Number_of_Veterans) as avg_Number_of_Veterans,
        AVG(td.Foreign_born) as avg_Foreign_born,
        td.State_Code AS demographics_state_code,  
        td.Race,
        YEAR(ti.arrive_date) as year,
        MONTH(ti.arrive_date) as month 
    FROM temp_immigration ti
    JOIN (
        SELECT 
            State_Code,
            AVG(Median_Age) as Median_Age,
            AVG(Male_Population) as Male_Population,
            AVG(Female_Population) as Female_Population,
            AVG(Total_Population) as Total_Population,
            AVG(Number_of_Veterans) as Number_of_Veterans,
            AVG(Foreign_born) as Foreign_born,
            MAX(State) as State,
            MAX(Race) as Race
        FROM temp_demographics
        GROUP BY State_Code
    ) td ON ti.state_code = td.State_Code
    GROUP BY 
        ti.cic_id, 
        ti.country_code_1,
        ti.country_code_2,
        ti.city_code,
        ti.arrive_date,
        ti.state_code, 
        ti.depart_date,
        ti.visa_code,
        ti.birth_year,
        ti.gender,
        ti.airline,
        ti.flight_number,
        td.State_Code,
        td.Race,
        YEAR(ti.arrive_date),
        MONTH(ti.arrive_date)
""")

immi_demographics.printSchema()

try:
    immi_demographics.write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join("./final/", "immi_demographics"))
    print("immi_demographics write completed successfully")
except Exception as e:
    print(f"Error writing immi_demographics table: {str(e)}")
     


# In[25]:


immi_demographics.printSchema()


# In[26]:


dim_immi_personel.printSchema()


# In[27]:


dim_immi_airline.printSchema()


# In[28]:


dim_arrive_calendar.printSchema()


# In[29]:


dim_depart_calendar.printSchema()


# In[30]:


demo_population_dim.printSchema()


# #### 4.2 Data Quality Checks

# In[31]:


# creating temporary sql tables for data quality checks
dim_immi_personel.createOrReplaceTempView("dim_immi_personel")
dim_immi_airline.createOrReplaceTempView("dim_immi_airline")
dim_arrive_calendar.createOrReplaceTempView("dim_arrive_calendar")
dim_depart_calendar.createOrReplaceTempView("dim_depart_calendar")
demo_population_dim.createOrReplaceTempView("demo_population_dim")
immi_demographics.createOrReplaceTempView("immi_demographics")


# #### 4.2.1 Count checks to ensure completeness and integrity constraints (primary key, merging condition)

# In[32]:


spark.sql("""
select 
count(*)
, count(distinct cic_id)
, count(cic_id)
from dim_immi_personel
""").show()


# In[33]:


spark.sql("""
select 
count(*)
, count(distinct travel_id)
, count(travel_id)
from dim_immi_airline
""").show()


# In[35]:


spark.sql("""
select 
count(*)
, count(distinct arrive_date)
, count(arrive_date)
from dim_arrive_calendar
""").show()


# In[36]:


spark.sql("""
select 
count(*)
, count(distinct depart_date)
, count(depart_date)
from dim_depart_calendar
""").show()


# In[37]:


spark.sql("""
select 
count(*)
, count(distinct demo_pop_id)
, count(demo_pop_id)
from demo_population_dim
""").show()


# In[38]:


spark.sql("""
    SELECT 
        COUNT(*) AS total_rows,
        COUNT( immigration_state_code),
        COUNT( demographics_state_code)
    FROM immi_demographics
""").show()


# #### 4.2.2 Further investigation on dim_depart_calendar:
# **The count of unique depart_date values is less than the total count suggests that there are duplicate depart_date values in the DataFrame. To investigate further and identify the duplicates, run the following Spark SQL query:**

# In[39]:


# check if depart_date is unique in dim_deaprt_calendar
spark.sql("""
SELECT depart_date, COUNT(*) AS count
FROM dim_depart_calendar
GROUP BY depart_date
HAVING COUNT(*) > 1
ORDER BY count DESC
""").show()  


# In[40]:


# check if depart_date has missing value
spark.sql("""
SELECT depart_date
FROM dim_depart_calendar
WHERE depart_date IS NULL OR depart_date = ''
""").show()                       


# #### Result explain:
# - The result indicates that there is only one row in dp_calendar DataFrame where the depart_date column is null. There are no rows where the depart_date column is an empty string ('').
# 
# - In order to  handle this null value, I will remove the row with the null value. Here's how to drop the row with a null value:
# ```dp_calendar = dp_calendar.filter(dp_calendar["depart_date"].isNotNull())```
# 

# #### 4.2.3 Schema validation check

# In[ ]:


# Check the schema validation
expected_schema = ["cic_id", "country_code_1", "country_code_2", "birth_year", "gender"]

sql_queries = [f"SELECT * FROM dim_immi_personel WHERE `{col}` IS NULL" for col in expected_schema]
null_check_queries = " UNION ALL ".join(sql_queries)

mismatch_df = spark.sql(null_check_queries)

if mismatch_df.count() > 0:
    print("Schema mismatch!")
else:
    print("Schema is valid.")


# #### The "Schema mismatch" error suggests that there is a mismatch between the expected schema and the actual schema of the dim_immi_personel table. To identify the issues, further investigation include :
# 1. Check if dim_immi_personel Exists.
# 2. Verify Data Types
# 3. Check for Missing Columns.
# 4. Check for NULL Values.

# #### 4.3 Data dictionary 
# Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file. -- please check Data_Dictionary.PDF file

# #### Step 5: Complete Project Write Up
# * Clearly state the rationale for the choice of tools and technologies for the project.
# * Propose how often the data should be updated and why.
# * Write a description of how you would approach the problem differently under the following scenarios:
#  * The data was increased by 100x.
#  * The data populates a dashboard that must be updated on a daily basis by 7am every day.
#  * The database needed to be accessed by 100+ people.

# #### 5.1 Tools and technologies for the project
# - AWS s3 : used for storaging and loading data and tables.
# - Pandas : used for small data exploration for data structure understanding
# - Apache Spark : used for larger data processing, including extracting, transformating and loading data warehouse tables.
# 
# #### 5.2 Frequency of Data Updates
# Given that the fact table immi_demographics is derived from immigration and demographics data, the update frequency largely depends on how often the underlying data changes. Here are some considerations:
# 
# - Immigration Data: If this data changes frequently (daily or weekly), fact table should be updated at the same frequency to ensure it reflects the most current trends and patterns.
# - Demographic Data: Demographic data doesn't change as rapidly. Updating it annually or semi-annually might be sufficient.
# 
# #### 5.3 Approach for Different Scenarios
# ##### a. Data Volume Increases by 100x
# * Scaling Infrastructure: Utilize a more powerful cluster with additional nodes to handle the increased data volume in Spark.
# * Optimize Data Storage: Use efficient file formats like Parquet, which is already in use, and consider partitioning and bucketing strategies to improve query performance.
# * Caching: For frequently accessed data, consider caching tables or specific queries in Spark.
# * Resource Management: Fine-tune resource allocation (e.g., memory, cores) to ensure efficient processing.
# * Archiving: Archive older data that's accessed less frequently to maintain system performance.
# 
# ##### b. Daily Dashboard Updates by 7 am
# * Workflow Scheduling: Use a workflow scheduler like Apache Airflow to manage the ETL pipeline, ensuring data is processed and ready before the 7 am deadline.
# * Incremental Loading: Instead of processing the entire dataset each day, use incremental loading techniques to process only new or changed data.
# * Monitoring and Alerts: Implement monitoring for the ETL process to quickly identify and resolve any issues that could delay the daily update.
# 
# ##### c. Database Accessed by 100+ People
# * Concurrency and Load Management: Use a database that can handle high concurrency, ensuring multiple users can query the data simultaneously without performance degradation.
# * Access Controls: Implement role-based access controls to manage who can view or modify the data.
# * Scalability: Ensure the underlying infrastructure can scale to support multiple users. This might involve scaling up the database or using a distributed database system.
# * Caching Popular Queries: Cache results of common queries to improve response times.
# * Usage Monitoring: Monitor database usage to identify bottlenecks and optimize performance where needed.
