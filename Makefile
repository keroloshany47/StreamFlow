.PHONY: up down reset submit-spark java-producer python-producer

up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v
	rm -rf mnt/checkpoints mnt/spark-state

submit-spark:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  --conf spark.jars.ivy=/tmp/ivy \
	  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
	  /opt/spark/jobs/spark_processor.py

java-producer:
	./gradlew run

python-producer:
	python Python_producer/Python_Producer.py
