import hashlib
from pyspark.sql import SparkSession

def mask_phi(value: str) -> str:
    if value:
        return hashlib.sha256(value.encode()).hexdigest()
    return value

def run_bronze_layer(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("BronzeLayer").getOrCreate()

    df = spark.read.json(input_path)

    if "patient_name" in df.columns:
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        mask_udf = udf(mask_phi, StringType())
        df = df.withColumn("patient_name", mask_udf(df["patient_name"]))

    df.write.mode("overwrite").json(output_path)  # 🔥 changed (no delta for now)

    print("Bronze layer completed")

if __name__ == "__main__":
    run_bronze_layer("data/raw", "data/bronze")