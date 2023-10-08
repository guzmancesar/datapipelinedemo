##main file for datapipelinedemo script


from pyspark.sql import SparkSession
from pyspark.sql.functions import when 

def main():
    # Creating a Spark session so we can use Spark
    spark = SparkSession.builder.appName("homemadePipeline").getOrCreate()

    # Suppress warning messages unrelated to the purpose of this exercise
    spark.sparkContext.setLogLevel("ERROR")

    # Finding a nice dataset!

    # What is a spark dataframe? https://spark.apache.org/docs/latest/sql-programming-guide.html
    # Header parameter - whether the first col of your csv are the column names, in this case, yes, set to true
    # InferSchema - should pyspark figure out the data types of your cols for your table schema
    df = spark.read.csv('fighters_index.csv', header=True, inferSchema=True)
    df.show()
    
    #lets clean this data uyp a little bit
    df = df.withColumn("age", when(df["age"] == "Unknown", None).otherwise(df["age"]))
    df = df.withColumn("height", when(df["height"] == "Unknown", None).otherwise(df["height"]))
    df = df.withColumn("reach", when(df["reach"] == "Unknown", None).otherwise(df["reach"]))

    df.unpersist()
    df.show()
    

    # Stopping the Spark session
    spark.stop()


main()

