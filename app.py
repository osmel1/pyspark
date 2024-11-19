from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, month, year, desc, coalesce
from pyspark.sql.types import DoubleType, StringType, DateType

# Initialisation de la session Spark
spark = SparkSession.builder.appName("Analyse des Ventes").getOrCreate()

# 1. Lisez le fichier CSV et affichez un aperçu des données
file_path = "/app/products.csv" 
data = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
data.show(5) 

# 2. Vérifiez le schéma des données et comptez le nombre de lignes
data.printSchema()
print("Nombre de lignes:", data.count())

# 3. Filtrez les transactions avec un montant supérieur à un certain seuil (par exemple, 100 €)
data_filtered = data.filter(col("amount") > 100)
data_filtered.show(5)

# 4. Remplacez les valeurs nulles dans les colonnes amount et category par des valeurs par défaut
data = data.fillna({"amount": 0.0, "category": "Unknown"})
data.show(5)

# 5. Convertissez la colonne date en un format de date pour permettre des analyses temporelles
data = data.withColumn("date", col("date").cast(DateType()))
data.show(5)

# 6. Calculez le total des ventes pour l'ensemble de la période
total_sales = data.agg(sum("amount").alias("total_sales"))
total_sales.show()

# 7. Calculez le montant total des ventes par catégorie de produit
sales_by_category = data.groupBy("category").agg(sum("amount").alias("total_sales")).orderBy(desc("total_sales"))
sales_by_category.show()

# 8. Calculez le montant total des ventes par mois
sales_by_month = data.groupBy(year("date").alias("year"), month("date").alias("month")).agg(sum("amount").alias("total_sales")).orderBy("year", "month")
sales_by_month.show()

# 9. Identifiez les 5 produits les plus vendus en termes de montant total
top_5_products = data.groupBy("product_id").agg(sum("amount").alias("total_sales")).orderBy(desc("total_sales")).limit(5)
top_5_products.show()

# 10. Pour chaque catégorie, trouvez le produit le plus vendu
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("category").orderBy(desc("total_sales"))
top_product_by_category = data.groupBy("category", "product_id").agg(sum("amount").alias("total_sales"))
top_product_by_category = top_product_by_category.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank")
top_product_by_category.show()

# Arrêter la session Spark
spark.stop()
