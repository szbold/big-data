from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, length

# 1. Inicjalizacja SparkSession
spark = SparkSession.builder.appName("ImionaDlugoscHistogram").getOrCreate()

try:
    # 2. Wczytanie danych z pliku CSV
    df_raw = spark.read.csv(
        "/opt/spark/app/data/popularnosc_imion.csv",
        header=True,
        sep=';',
        inferSchema=True
    )
    print("✅ Dane surowe:")
    df_raw.show(5)

    # 3. Filtrowanie błędnych danych (null w 'imie' lub 'liczba')
    df_cleaned = df_raw.filter(col("imie").isNotNull() & col("liczba").isNotNull())
    print("✅ Dane po oczyszczeniu:")
    df_cleaned.show(5)

    # 4. Dodanie kolumny: długość imienia
    df_with_length = df_cleaned.withColumn("dlugosc_imienia", length("imie"))
    print("✅ Dodana kolumna 'dlugosc_imienia':")
    df_with_length.select("imie", "dlugosc_imienia", "liczba").show(5)

    # 5. Grupowanie po długości i zliczanie liczby nadanych imion
    df_grouped = df_with_length.groupBy("dlugosc_imienia") \
        .agg(spark_sum("liczba").alias("suma_nadanych"))
    print("✅ Wynik grupowania:")
    df_grouped.show()

    # 6. Sortowanie po długości (rosnąco)
    df_sorted = df_grouped.orderBy("dlugosc_imienia")
    print("✅ Wynik posortowany po długości imienia:")
    df_sorted.show()

    # 7. Buforowanie przetworzonego DataFrame
    df_final = df_sorted.cache()

    # 8. Zapis wyniku do pliku CSV
    df_final.write.mode("overwrite").csv("/opt/spark/app/output/histogram_dlugosc_imion.csv", header=True)
    print("✅ Zapisano wynik do: /opt/spark/app/output/histogram_dlugosc_imion.csv")

except Exception as e:
    print(f"❌ Błąd: {e}")
    spark.stop()
    exit(1)
