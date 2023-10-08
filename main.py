##main file for datapipelinedemo script


from pyspark.sql import SparkSession


def main():
	
	#creating a spark session so we can use spark
	spark = SparkSession.builder.appName("homemadePipeline").getOrCreate()
	print('hello this is working!')
	#stopping the spark session
	spark.stop()


main()

