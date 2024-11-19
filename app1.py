from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("TP1 spark").getOrCreate()
df1=spark.read.option("header",True).csv("/app/products.csv")

df1.printSchema()
df1.show()