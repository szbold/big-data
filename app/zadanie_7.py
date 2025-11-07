from pyspark.sql import SparkSession

def main():
    print("Uruchamianie aplikacji Spark (zadanie_7_tabela)...")
    spark = SparkSession.builder \
        .appName("Tworzenie_Tabeli_BI") \
        .enableHiveSupport() \
        .getOrCreate()

    print("SparkSession z Hive wsparciem została utworzona.")

    dane = [
        (1, "Anna", "Nowak", 34, "HR"),
        (2, "Marek", "Kowalski", 25, "IT"),
        (3, "Ewa", "Lis", 45, "Finanse"),
        (4, "Piotr", "Zając", 30, "IT"),
        (5, "Jan", "Kot", 52, "Finanse"),
        (6, "Maria", "Wilk", 28, "HR")
    ]
    kolumny = ["id_pracownika", "imie", "nazwisko", "wiek", "departament"]
    pracownicy_df = spark.createDataFrame(dane, kolumny)
    print("DataFrame został stworzony.")

    print("Rozpoczynanie zapisu do tabeli 'pracownicy_tabela'...")
    pracownicy_df.write.mode("overwrite").saveAsTable("pracownicy_tabela")
    print("Sukces! Tabela 'pracownicy_tabela' została utworzona.")
    spark.stop()

if __name__ == "__main__":
    main()