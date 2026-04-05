# 中級レベルのPySpark操作
# RDD操作、スキーマ定義、ウィンドウ関数を組み合わせた応用例

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, sum

# Sparkセッションの作成
spark = SparkSession.builder.appName("IntermediateOperations").getOrCreate()

print("=== RDDからDataFrameへの変換 ===")
# サンプルデータの作成
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]

# RDDの作成
rdd = spark.sparkContext.parallelize(data)

# RDDからデータフレームを作成
df = rdd.toDF(["Name", "Age"])

# データフレームの表示
df.show()

print("\n=== スキーマ定義とウィンドウ関数 ===")
# 売り上げデータのスキーマ定義
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Sales", IntegerType(), True)
])

# サンプルデータ
sales_data = [("2024-02-10", "ProductA", 100),
              ("2024-02-10", "ProductB", 150),
              ("2024-02-10", "ProductC", 200),
              ("2024-02-11", "ProductA", 120),
              ("2024-02-11", "ProductB", 180),
              ("2024-02-11", "ProductC", 220)]

# RDDの作成
sales_rdd = spark.sparkContext.parallelize(sales_data)

# RDDからデータフレームを作成（スキーマ指定）
sales_df = spark.createDataFrame(sales_rdd, schema)

# データフレームの表示
sales_df.show()

# 日付ごとに商品の売り上げランキングを計算
window_spec = Window.partitionBy("Date").orderBy(col("Sales").desc())
ranked_df = sales_df.withColumn("Rank", rank().over(window_spec))

print("\n=== 日付ごとの売り上げランキング ===")
ranked_df.show()

# 追加: 日付ごとの合計売り上げも計算
daily_total = sales_df.groupBy("Date").agg(sum("Sales").alias("Total_Sales"))
print("\n=== 日付ごとの合計売り上げ ===")
daily_total.show()

# Sparkセッションの終了
spark.stop()