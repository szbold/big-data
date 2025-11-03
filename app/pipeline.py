from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, rank, desc, when, lit, count
from pyspark.sql.window import Window

# 1. Inicjalizacja SparkSession
# Spark automatycznie wykryje Mastera dzięki --master w spark-submit
spark = SparkSession.builder.appName("ImionaAnalyticsPipeline").getOrCreate()

# Ustawienia do zapisu do PostgreSQL
JDBC_URL = "jdbc:postgresql://postgres:5432/analytics_db" # 'postgres' to nazwa serwisu
CONNECTION_PROPERTIES = {
            "user": "user",
                "password": "password",
                    "driver": "org.postgresql.Driver"
                    }

# 2. Wczytanie i Wstępne Przetwarzanie (Zadanie 1)
try:
        df_imiona = spark.read.csv(
                        "/opt/spark/app/data/popularnosc_imion.csv", # Ścieżka wewnątrz kontenera
                                header=True,
                                sep=';',
                                        inferSchema=False, # Zawsze lepiej zdefiniować ręcznie
                                                schema="imie STRING, plec STRING, liczba STRING" # Wstępnie wszystko jako STRING
                                                    ).withColumn("Liczba", col("Liczba").cast("integer")) # Castowanie do Integer
            
            # Proste oczyszczanie: usunięcie wierszy z brakiem Liczby lub Liczba <= 0
        df_imiona = df_imiona.filter(col("liczba").isNotNull() & (col("liczba") > 0))
                    
except Exception as e:
        print(f"Błąd podczas ładowania danych: {e}")
        spark.stop()
        exit(1)

df_imiona.printSchema()
imiona_count = df_imiona.count()
print(f"Liczba wierszy w DataFrame po wczytaniu i oczyszczaniu: {imiona_count}")
if imiona_count == 0:
        print("BŁĄD: Wczytany DataFrame jest PUSTY! Sprawdź ścieżkę do CSV i separator.")
        spark.stop()
        exit()

# 3. Zaawansowana Transformacja i Ranking (Zadanie 2 & 3)
# Obliczenie rankingu dla każdej płci
window_spec = Window.partitionBy("plec").orderBy(desc("liczba"))
df_ranking = df_imiona.withColumn("ranking", rank().over(window_spec))


# Obliczenie globalnej popularności imion (np. sumaryczna popularność wszystkich imion)
df_total = df_imiona.agg(spark_sum("liczba").alias("Suma_Nadanych_Imion"))
total_imion = df_total.collect()[0]["Suma_Nadanych_Imion"]
print(f"Całkowita liczba nadanych imion: {total_imion}")

# Filtrowanie Top 10 (przykład finalnego zbioru do zapisu)
df_top10 = df_ranking.filter(col("Ranking") <= 10).select("imie", "plec", "liczba", "ranking")



df_top10_count = df_top10.count()
print(f"Liczba wierszy w DataFrame do zapisu (Top 10): {df_top10_count}")
df_top10.show(10, truncate=False) # Pokaż pierwszych 10 wierszy


# 4. Zapis Wyników do PostgreSQL (Zadanie 4)
print("Zapisywanie Top 10 do PostgreSQL...")
try:
        df_top10.write.jdbc(
                        url=JDBC_URL,
                                table="top10_imiona_2025", # Nazwa tabeli w Postgresie
                                        mode="overwrite",          # Tryb zapisu
                                                properties=CONNECTION_PROPERTIES
                                                    )
        print("Zapis zakończony sukcesem!")
except Exception as e:
        print(f"BŁĄD ZAPISU DO POSTGRESQL: Sprawdź połączenie i sterownik JDBC. Szczegóły: {e}")
        spark.stop()
