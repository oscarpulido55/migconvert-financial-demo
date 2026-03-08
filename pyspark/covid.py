from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


sparkSession = SparkSession.builder.appName("rwHDFS").getOrCreate()
sparkcont = SparkContext.getOrCreate(SparkConf().setAppName("rwHDFS"))

# Read csv file from HDFS as DataFrame
df = spark.read.csv('hdfs://namenode:8020/covi/csv_covi19',inferSchema = True, header = True)

# Read json file from HDFS as DataFrame
df2 = sparkSession.read.json('hdfs://namenode:8020/covi/json_covi19')

#To verify DataFrame type
df.dtypes

#To show the first element of your DataFrame
df.show()

#To print the number of record on your dataset
df.count()

#Show n first observation
df.head(5)

#Get the summary statistic (mean, standard deviation, min, max, count)
df.describe().show()

#Describe a particular column
df.describe('deaths').show()

#Get the DF which will not have duplicate rows of given DataFrame
df.select('countriesAndTerritories').dropDuplicates().show()

#Display columns names
df.columns

#Filter data and create other DF
covi_colombia=df.filter(df.countriesAndTerritories=='Colombia').show()

#get count
covi_colombia.count()

#Filter Data multiple parameters
df.filter((df.continentExp=='America') & (df.day=='8')).show()

#Sorting Data (OrderBy)
df.orderBy(df.deaths).show()

#Save Data to HDFS
covi_colombia.write.format("csv").option('header',True).mode('overwrite').option('sep',',').save('hdfs://namenode:8020/covi/csv_covi_colombia')
# To summit spark-submit --master yarn --deploy-mode client <py file>