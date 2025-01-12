# Spark app for ETL from .csv to s3

Spark (v3.2.0) приложение `app.py` для ETL-обработки сырых данных из .csv формата и выгрузки витрины с аггрегированными данными в s3-совместимое хранилище (используется minio) в parquet.

Порядок запуска приложения:
```
git clone https://github.com/zvezdochetast/spark_de_hw.git
cd spark_de_hw
docker build -t spark-app:dev .
docker-compose up -d
```

По умолчанию приложение запускается с драйвером `PYSPARK_DRIVER_PYTHON=python`. Предусмотрена возможность
запуска в dev-режиме для разработки и отладки в ноутбуках jupyter, сохраняемых в папке `/notebooks`. 

Для запуска в dev-режиме необходимо раскомментировать в файле окружения `.env` переменные:
```
- PYSPARK_DRIVER_PYTHON=notebook # For develop app
- PYSPARK_DRIVER_PYTHON_OPTS="--ip=0.0.0.0 --port=8888 --no-browser --allow-root" # For develop app
```

Входные данные находятся в файле `/data/crimes.csv`.
Выходные данные находятся в папке  `/minio-data/spark-data-pub/output/dir/bi_data.parquet`.
![Иллюстрация выходных данных](https://github.com/zvezdochetast/spark_de_hw/raw/master/images/output.png)

