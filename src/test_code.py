# from pyspark.sql.functions import * と書く事も多い。
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


filename = 'test.csv'
spark = SparkSession.builder \
    .master("local") \
    .appName("app") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# sepを；にすると、全ての列が一つにまとめられてしまう。
data = spark.read.csv(filename, header=True, inferSchema=True, sep=',')
data.show()

name = data.select(
    "date",
    F.col("name").alias("Name")
)
name.show()

print(name.collect())

data.registerTempTable('users')
df = spark.sql(
        'SELECT * FROM users'
    ).withColumn(
        'spark_user', F.lit(True)
    ).drop(
        'score'
    )

df.show()

Sumtable = data.groupby(
    "name"
).sum("score")

Sumtable.show()

l = [('Alice', 1)]
T = spark.createDataFrame(l, ['name', 'age']).collect()
print(T)



