from pyspark.sql import SparkSession
from pyspark.sql.functions import count

def run_gold_layer(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("GoldLayer").getOrCreate()

    df = spark.read.format("delta").load(input_path)

    # Example aggregation
    if "medicine_name" in df.columns:
        df_gold = df.groupBy("medicine_name").agg(count("*").alias("total_records"))
    else:
        df_gold = df

    df_gold.write.format("delta").mode("overwrite").save(output_path)

    print("Gold layer completed")

if __name__ == "__main__":
    run_gold_layer("data/silver", "data/gold")