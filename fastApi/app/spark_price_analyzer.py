from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window



# 1. Spark Session (MinIO/S3 연동 설정 포함)
spark = SparkSession.builder \
    .appName("CoupangLowestPriceAnalyzer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. MinIO의 전체 쿠팡 가격 Parquet 데이터 읽기
df = spark.read.parquet("s3a://raw-coupang-prices/*/*/*.parquet")

# 3. Window 함수를 이용하여 상품별 역대 최저가 및 평균가 계산
product_window = Window.partitionBy("product_id")

analyzed_df = df.withColumn("historical_min_price", F.min("price").over(product_window)) \
                .withColumn("avg_price", F.avg("price").over(product_window))

# 4. 가장 최신 수집 시점의 데이터만 추출
latest_window = Window.partitionBy("product_id").orderBy(F.col("collected_at").desc())
latest_df = analyzed_df.withColumn("rank", F.row_number().over(latest_window)) \
                       .filter(F.col("rank") == 1) \
                       .drop("rank")

# 5. [알림 조건] 현재가가 역대 최저가와 같거나 더 낮은 경우 필터링
alert_targets = latest_df.filter(F.col("price") <= F.col("historical_min_price"))

print("=== 🚨 오늘 역대 최저가 달성 상품 목록 🚨 ===")
alert_targets.select("product_name", "price", "historical_min_price", "avg_price", "collected_at").show(truncate=False)

# (이후 alert_targets의 결과를 텔레그램/카카오톡 API 등으로 전송하는 함수 호출)