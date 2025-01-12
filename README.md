# Spark app for ETL from .csv to s3
Homework for DE course 

По умолчанию приложение запускается с драйвером `PYSPARK_DRIVER_PYTHON=python`.
Для запуска в dev-режиме в ноутбуках jupyter необходимо раскомментировать в файле окружения `.env` переменные:
- PYSPARK_DRIVER_PYTHON=notebook # For develop app
- PYSPARK_DRIVER_PYTHON_OPTS="--ip=0.0.0.0 --port=8888 --no-browser --allow-root" # For develop app
В dev-режиме рабочий код находится в папке `/notebooks`. 

Входные данные находятся в файле `/data/crimes.csv`.
Выходные данные находятся в папке  `/minio-data/spark-data-pub/output/dir/bi_data.parquet`.
![Иллюстрация выходных данных](https://github.com/zvezdochetast/spark_de_hw/raw/master/images/output.png)


