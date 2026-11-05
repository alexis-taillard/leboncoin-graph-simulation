from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from graphframes import GraphFrame


spark = SparkSession.builder \
    .appName("LeBonCoinSimulation") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# créé 4 partitions pour pouvoir répartir le travail sur 4 coeurs (parallélisme)

spark.sparkContext.setLogLevel("ERROR") # on affiche que les erreurs importantes


schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_city", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_cat", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("price", DoubleType(), True)
])


stream_df = spark.readStream \
    .schema(schema) \
    .json("events")

stream_df = stream_df.withColumn(
    "timestamp",
    to_timestamp("timestamp")
)

stream_df = stream_df.withWatermark(
    "timestamp",
    "30 seconds"
)

def process_batch(batch_df, batch_id):

    print(f"\n========== BATCH {batch_id} ==========\n")

    if batch_df.isEmpty():
        print("Batch vide")
        return

    # =========================
    # FENETRES TEMPORELLES
    # =========================

    print("\n--- ACTIONS PAR FENETRE ---")

    window_stats = batch_df.groupBy(
        window(col("timestamp"), "30 seconds"),
        col("action_type")
    ).count()

    window_stats.show(truncate=False)

    # =========================
    # VERTICES
    # =========================

    vertices_users = batch_df.select(
        col("user_id").alias("id")
    ).distinct().withColumn(
        "type",
        lit("USER")
    )

    vertices_products = batch_df.select(
        col("product_id").alias("id")
    ).distinct().withColumn(
        "type",
        lit("PRODUCT")
    )

    vertices_sellers = batch_df.select(
        col("seller_id").alias("id")
    ).distinct().withColumn(
        "type",
        lit("SELLER")
    )

    vertices = vertices_users \
        .union(vertices_products) \
        .union(vertices_sellers)

    # =========================
    # EDGES
    # =========================

    user_product_edges = batch_df.select(
        col("user_id").alias("src"),
        col("product_id").alias("dst"),
        col("action_type").alias("relationship")
    )

    seller_product_edges = batch_df.select(
        col("seller_id").alias("src"),
        col("product_id").alias("dst")
    ).withColumn(
        "relationship",
        lit("PROPOSE")
    )

    edges = user_product_edges.union(
        seller_product_edges
    )

    # =========================
    # GRAPHFRAME
    # =========================

    graph = GraphFrame(vertices, edges)

    print("\n--- VERTICES ---")
    graph.vertices.show(truncate=False)

    print("\n--- EDGES ---")
    graph.edges.show(truncate=False)

    # =========================
    # DEGREES
    # =========================

    degrees = graph.degrees

    print("\n--- DEGREES ---")
    degrees.show(truncate=False)

    # =========================
    # EXPORT
    # =========================

    vertices.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("output/vertices")

    edges.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("output/edges")

    degrees.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("output/degrees")

    windows_export = window_stats.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("action_type"),
        col("count")
    )

    windows_export.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("output/windows")

    print("\nEXPORT TERMINE")


query = stream_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()