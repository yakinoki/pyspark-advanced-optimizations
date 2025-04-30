from pyspark.sql import SparkSession

# Sparkセッションの作成
spark = SparkSession.builder.appName("example").getOrCreate()

# サンプルデータの作成
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]

# RDDの作成
rdd = spark.sparkContext.parallelize(data)

# RDDからデータフレームを作成
df = rdd.toDF(["Name", "Age"])

# データフレームの表示
df.show()
