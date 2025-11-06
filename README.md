###

Jak odpalic

```bash
docker compose up -d
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/app/postgresql-42.7.7.jar /opt/spark/app/pipeline.py
docker exec -it postgres psql -U user -d analytics_db -c "SELECT * FROM top10_imiona_2025;

/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/app/name_analysis.py - do części 3 :)
```


Część 4
```bash
docker compose up -d
docker exec -it spark-master bash

/opt/spark/bin/spark-submit /opt/spark/app/name_analysis.py 
/opt/spark/bin/spark-submit /opt/spark/app/4_zapis_danych.py 
```