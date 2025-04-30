# PySparkから必要なライブラリをインポート
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# CSVファイルのファイル名を指定
filename = 'test.csv'

# Sparkセッションを作成
spark = SparkSession.builder \
    .master("local") \
    .appName("app") \
    .getOrCreate()

# CSVファイルをDataFrameに読み込む
data = spark.read.csv(filename, header=True, inferSchema=True, sep=',')
data.show()

# 特定の列を選択し、別名を付ける
name = data.select(
    "date",
    F.col("name").alias("Name")
)
name.show()

# 選択したデータを収集して表示
print(name.collect())

# DataFrameに一時的なビューを作成
data.createOrReplaceTempView('users')

# DataFrameに対してSQLクエリを実行し、1つの列を削除
df = spark.sql('SELECT *, TRUE as spark_user FROM users').drop('score')
df.show()

# 'name'列でグループ化し、'score'列の合計を計算
sum_table = data.groupBy("name").sum("score")
sum_table.show()

# 指定されたデータと列名で新しいDataFrame 'T'を作成
l = [('Alice', 1)]
T = spark.createDataFrame(l, ['name', 'age']).collect()
print(T)
