###

Jak odpalic

```bash
docker compose up -d
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/app/postgresql-42.7.7.jar /opt/spark/app/pipeline.py
docker exec -it postgres psql -U user -d analytics_db -c "SELECT * FROM top10_imiona_2025;
```
