# PySparkの最適化を試してみる

個人的にPySparkの最適化について色々コードを書いて実験しているリポジトリです。

## 主な内容

### シャッフル最適化
- アダプティブクエリ実行
- パーティションのcoalesce
- データ移動の最小化

### パーティション設計
- 動的パーティション調整
- 偏ったデータのためのrepartition

### Skew（データ偏り）対策
- Saltingテクニック
- 偏ったキーの再分散

### Join戦略
- Broadcast join
- Sort-merge join

## プロジェクト構造

```
pyspark-advanced-optimizations/
├── src/
│   ├── basic_operations.py          # 基礎的な操作
│   ├── intermediate_operations.py   # 応用的な操作
│   └── advanced_optimizations.py    # 最適化の実験
├── data/                            # サンプルデータ
└── README.md
```

## セットアップ
- [WindowsでのPySpark環境構築](https://qiita.com/naoya_ok/items/40e5209e384fe776307c)
- Python 3.8+
- Apache Spark 3.0+
- Java 8+

### 設定例
```python
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
```

## コード例

### シャッフル最適化
```python
df_optimized = df.groupBy("key").agg(F.sum("value")).coalesce(4)
```

### Skew対策
```python
df_salted = df.withColumn("salt", F.floor(F.rand() * num_salts)) \
    .withColumn("salted_key", F.concat(F.col("key"), F.lit("_"), F.col("salt")))
```

### Join
```python
result = large_df.join(F.broadcast(small_df), "key")
```

## 参考サイト

- [Parquet形式ファイルの作成方法](https://qiita.com/naoya_ok/items/4fa2cdd5d968e977599b)
- [Apache Sparkパフォーマンスチューニングガイド](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Databricksパフォーマンスベストプラクティス](https://docs.databricks.com/sql/performance.html)