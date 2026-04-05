# Advanced PySpark Performance Optimizations
# Demonstrating shuffle optimization, partition design, skew handling, and join strategies

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Optimized SparkSession configuration
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("AdvancedPySparkOptimizations") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Sample data creation with potential skew
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("date", StringType(), True)
])

# Create skewed data (some users have many more records)
data = []
for i in range(1000):
    user = f"user_{i % 10}"  # Skew: users 0-9 have 100 records each
    for j in range(10 if i < 100 else 1):
        data.append((user, f"product_{j}", (i + j) % 100, f"2024-01-{str((i % 30) + 1).zfill(2)}"))

df = spark.createDataFrame(data, schema)

print("Original DataFrame partitions:", df.rdd.getNumPartitions())
df.show(10)

# Partition Design: Repartition for better distribution
df_repartitioned = df.repartition(16, "user_id")  # Increase partitions and distribute by skewed column
print("After repartition:", df_repartitioned.rdd.getNumPartitions())

# Skew Handling: Salting technique for aggregation
# Add salt to distribute skewed keys
df_salted = df_repartitioned.withColumn("salt", F.floor(F.rand() * 4)) \
    .withColumn("salted_user", F.concat(F.col("user_id"), F.lit("_"), F.col("salt")))

sum_salted = df_salted.groupBy("salted_user").agg(F.sum("score").alias("total_score"))
sum_desalted = sum_salted.withColumn("user_id", F.split("salted_user", "_")[0]) \
    .groupBy("user_id").agg(F.sum("total_score").alias("final_score"))

print("Skew-handled aggregation:")
sum_desalted.show()

# Shuffle Optimization: Coalesce to reduce partitions after operations
df_coalesced = df_repartitioned.coalesce(4)
print("After coalesce:", df_coalesced.rdd.getNumPartitions())

# Join Strategy: Broadcast Join
# Create small dimension table
products = spark.createDataFrame([
    ("product_0", "Electronics"),
    ("product_1", "Books"),
    ("product_2", "Clothing")
], ["product", "category"])

# Force broadcast join (normally Spark decides, but we can hint)
joined = df_repartitioned.join(F.broadcast(products), "product", "left")
print("Broadcast join result:")
joined.show(10)

# Advanced: Window functions with partition optimization
from pyspark.sql.window import Window
window_spec = Window.partitionBy("user_id").orderBy(F.desc("score"))
ranked = df_repartitioned.withColumn("rank", F.rank().over(window_spec))
print("Ranked data:")
ranked.show(20)

# Cache for iterative operations
df_cached = df_repartitioned.cache()
print("Cached DataFrame count:", df_cached.count())

# Persist with different storage levels
from pyspark import StorageLevel
df_persist_memory = df_repartitioned.persist(StorageLevel.MEMORY_ONLY)
df_persist_disk = df_repartitioned.persist(StorageLevel.DISK_ONLY)
df_persist_both = df_repartitioned.persist(StorageLevel.MEMORY_AND_DISK)

print("Persisted DataFrame (MEMORY_ONLY) count:", df_persist_memory.count())
print("Persisted DataFrame (DISK_ONLY) count:", df_persist_disk.count())
print("Persisted DataFrame (MEMORY_AND_DISK) count:", df_persist_both.count())

# Checkpoint for fault tolerance (saves to disk)
spark.sparkContext.setCheckpointDir("checkpoint_dir")
df_checkpointed = df_repartitioned.checkpoint()
print("Checkpointed DataFrame count:", df_checkpointed.count())

# Unpersist to free memory
df_cached.unpersist()
df_persist_memory.unpersist()
print("Unpersisted cached DataFrames")

# Broadcast variables for efficient sharing
broadcast_var = spark.sparkContext.broadcast({"key1": "value1", "key2": "value2"})
print("Broadcast variable:", broadcast_var.value)

# Memory management: coalesce after heavy operations
df_final = df_repartitioned.groupBy("user_id").agg(F.sum("score")).coalesce(2)
print("Final optimized DataFrame partitions:", df_final.rdd.getNumPartitions())

# Cleanup
spark.stop()
