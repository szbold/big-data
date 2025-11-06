from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import os

# 1️. Inicjalizacja SparkSession
spark = SparkSession.builder \
    .appName("ZapisOptymalnyDanychHistogramu") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

try:
    base_path = "/opt/spark/app/output"
    stage1 = f"{base_path}/JK_4/etap1_wczytanie_csv"
    stage2 = f"{base_path}/JK_4/etap2_dodanie_przedzialow"
    stage3 = f"{base_path}/JK_4/etap3_zapis_parquet"
    stage4 = f"{base_path}/JK_4/etap4_odczyt_parquet"


    # 2️. Wczytanie histogramu z CSV
    input_path = f"{base_path}/histogram_dlugosc_imion.csv"
    df_hist = spark.read.csv(input_path, header=True, inferSchema=True)
    print("\n[ETAP 1] Wczytano dane histogramu:")
    df_hist.show(5)
    os.system(f"rm -rf {stage1}")
    df_hist.write.mode("overwrite").csv(stage1, header=True)

    # 3️. Dodanie kolumny przedziałów długości
    df_partitioned = df_hist.withColumn(
        "przedzial_dlugosci",
        when(col("dlugosc_imienia") <= 4, "krótkie")
        .when((col("dlugosc_imienia") >= 5) & (col("dlugosc_imienia") <= 7), "średnie")
        .otherwise("długie")
    )
    print("\n[ETAP 2] Dodano kolumnę 'przedzial_dlugosci':")
    df_partitioned.show(5)
    os.system(f"rm -rf {stage2}")
    df_partitioned.write.mode("overwrite").csv(stage2, header=True)

    # 4️. Zapis w formacie Parquet z partycjonowaniem
    os.system(f"rm -rf {stage3}")
    (
        df_partitioned
        .write
        .mode("overwrite")
        .partitionBy("przedzial_dlugosci")
        .parquet(stage3)
    )
    print(f"\n[ETAP 3] Dane zapisano w formacie PARQUET (z partycjonowaniem) do: {stage3}")

    # 5️. Odczyt Parquet i zapis wyniku końcowego do CSV
    df_check = spark.read.parquet(stage3)
    print("\n[ETAP 4] Odczyt z formatu Parquet:")
    df_check.show(10)
    os.system(f"rm -rf {stage4}")
    df_check.write.mode("overwrite").csv(stage4, header=True)


except Exception as e:
    print(f"Błąd: {e}")

finally:
    spark.stop()
