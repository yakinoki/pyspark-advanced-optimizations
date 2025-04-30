from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

# Sparkセッションの作成
spark = SparkSession.builder.appName("SalesRankingExample").getOrCreate()

# サンプルデータの作成（日付、商品名、売り上げ）
data = [("2024-02-10", "ProductA", 100),
        ("2024-02-10", "ProductB", 150),
        ("2024-02-10", "ProductC", 200),
        ("2024-02-11", "ProductA", 120),
        ("2024-02-11", "ProductB", 180),
        ("2024-02-11", "ProductC", 220)]

# RDDの作成
rdd = spark.sparkContext.parallelize(data)

# RDDの内容を表示してデータの形式を確認
print("RDDの内容:")
for row in rdd.collect():
    print(row)


# スキーマの定義
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Sales", IntegerType(), True)
])

# RDDの作成
rdd = spark.sparkContext.parallelize(data)

# RDDからデータフレームを作成
df = spark.createDataFrame(rdd, schema)

# 途中結果を出力
print("DataFrameの作成完了")
df.show()

# 日付ごとに商品の売り上げランキングを計算して新しい列を追加
window_spec = Window.partitionBy("Date").orderBy(col("Sales").desc())
ranked_df = df.withColumn("Rank", rank().over(window_spec))

# 結果の表示
print("日付ごとの商品の売り上げランキングの計算完了")
ranked_df.show()
