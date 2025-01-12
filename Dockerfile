FROM gcr.io/datamechanics/spark:platform-3.2.0-dm17

USER root

# Установим рабочую директорию для установки
WORKDIR /opt/spark/

# Установим переменные окружения для Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Убедимся, что все пути корректно настроены
RUN echo "SPARK_HOME=$SPARK_HOME" && echo $PATH

# Install PySpark
RUN pip install --upgrade pip
RUN pip3 install pyspark==3.2.0

# Скопируем доп зависимости и установим их
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY app.py .

# Set the default command to run the Spark application
# CMD ["spark-submit", "/opt/spark/app.py"]

# Открываем порты
EXPOSE 7077 8888 4040

# Команда для запуска Jupyter 
CMD jupyter-lab --allow-root --no-browser --ip=0.0.0.0