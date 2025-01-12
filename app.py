import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from functools import reduce  
from pyspark.sql.window import Window

# Загрузка данных
def load_data(input_path):
    return spark.read.option("header", "true").csv(input_path, inferSchema=True)

# Очистка данных
def clean_boston_crime_data(df: DataFrame) -> DataFrame:
    """
    Очищает данные о преступлениях в Бостоне:
    - Заполняет пропуски в столбце 'SHOOTING'.
    - Преобразует 'OCCURRED_ON_DATE' в формат timestamp.
    - Разделяет 'Location' на 'Lat' и 'Long'.
    - Проверяет допустимость значений координат.
    - Удаляет дубликаты по 'INCIDENT_NUMBER'.
    - Проверяет и фильтрует значения в 'UCR_PART'.
    
    Parameters:
    df (DataFrame): Исходный DataFrame с данными о преступлениях.
    
    Returns:
    DataFrame: Очищенный DataFrame.
    """
    
    # Заполнение пропусков в столбце SHOOTING значением False
    df_cleaned = df.fillna({'SHOOTING': 'False'})
    
    # Преобразование столбца OCCURRED_ON_DATE в правильный формат даты
    df_cleaned = df_cleaned.withColumn("OCCURRED_ON_DATE", F.to_timestamp("OCCURRED_ON_DATE", "yyyy-MM-dd HH:mm:ss"))
    
    # Разделение Location на Lat и Long
    df_cleaned = df_cleaned.withColumn("Lat", F.regexp_extract("Location", r"\((.*),", 1).cast("double")) \
                           .withColumn("Long", F.regexp_extract("Location", r", (.*)\)", 1).cast("double"))
    
    # Проверка значений Lat и Long (широта от -90 до 90, долгота от -180 до 180)
    df_cleaned = df_cleaned.filter((df_cleaned.Lat >= -90) & (df_cleaned.Lat <= 90) &
                                   (df_cleaned.Long >= -180) & (df_cleaned.Long <= 180))
    
    # Удаление дубликатов по INCIDENT_NUMBER
    df_cleaned = df_cleaned.dropDuplicates(subset=["INCIDENT_NUMBER"])
    
    # Проверка на корректность значений в столбце UCR_PART (должны быть только "Part One", "Part Two", "Part Three", "Other")
    valid_ucr_parts = ["Part One", "Part Two", "Part Three", "Other"]
    df_cleaned = df_cleaned.filter(df_cleaned.UCR_PART.isin(valid_ucr_parts))

    # Столбцы для проверки на None
    columns_to_check = ['UCR_PART', 'Lat', 'Long', 'DISTRICT']

    # Формирование условия для фильтрации строк с None в указанных столбцах
    condition = reduce(lambda acc, col: acc & df[col].isNotNull(), columns_to_check, F.lit(True))

    # Применение фильтрации
    df_cleaned = df_cleaned.filter(condition)
    
    return df_cleaned


def create_bi_dataset(df):
    # Извлечение первой части названия преступления (crime_type) из столбца 'OFFENSE_DESCRIPTION'
    df = df.withColumn('crime_type', F.split(df['OFFENSE_DESCRIPTION'], ' - ')[0])
    
    # Рассчитываем общее количество преступлений по районам
    crimes_total = df.groupBy('DISTRICT').agg(F.count('*').alias('crimes_total'))
    
    # Медиана числа преступлений в месяц в каждом районе
    crimes_monthly = df.groupBy('DISTRICT', 'YEAR', 'MONTH').agg(F.count('*').alias('monthly_crimes'))
    crimes_monthly = crimes_monthly.groupBy('DISTRICT').agg(F.expr('percentile_approx(monthly_crimes, 0.5)').alias('crimes_monthly'))
    
    # Три самых частых типа преступлений по району
    frequent_crime_types = df.groupBy('DISTRICT', 'crime_type').agg(F.count('*').alias('crime_type_count'))
    frequent_crime_types = frequent_crime_types.withColumn('rank', F.row_number().over(Window.partitionBy('DISTRICT').orderBy(F.desc('crime_type_count'))))
    frequent_crime_types = frequent_crime_types.filter(frequent_crime_types['rank'] <= 3)
    frequent_crime_types = frequent_crime_types.groupBy('DISTRICT').agg(F.concat_ws(', ', F.collect_list('crime_type')).alias('frequent_crime_types'))
    
    # Средняя широта и долгота для района
    avg_coordinates = df.groupBy('DISTRICT').agg(
        F.avg('Lat').alias('lat'),
        F.avg('Long').alias('lng')
    )
    
    # Объединение всех агрегаций в одну витрину
    dashboard_data = crimes_total.join(crimes_monthly, on='DISTRICT', how='inner') \
                            .join(frequent_crime_types, on='DISTRICT', how='inner') \
                            .join(avg_coordinates, on='DISTRICT', how='inner')

    return dashboard_data

def save_dataset_to_s3(df, bucket_name, path="output"):
    try:
        filename = "bi_data.parquet"
        full_output_path = f"s3a://{bucket_name}/{path}/{filename}"
        df.write.parquet(full_output_path)
        print(f"Data successfully uploaded to {full_output_path}")
    except Exception as e:
        print(f"Error occurred while saving to MinIO: {e}")

# Получение данных для s3
access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
endpoint = os.getenv("AWS_S3_ENDPOINT", "")
bucket_name = os.getenv("AWS_S3_BUCKET_PUB", "output")
region = os.getenv("AWS_REGION", "us-east-1")

# Создание SparkSession
spark = SparkSession.builder \
    .appName("BostonCrimeAnalysis") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true")  \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.region", region) \
    .getOrCreate()

# Параметры вызова приложения
input_path = sys.argv[1]
output_path = sys.argv[2]

# Загрузка
df_crimes = load_data(input_path)

# Очистка
df_cleaned = clean_boston_crime_data(df_crimes)

# Создание витрины
bi_dataset = create_bi_dataset(df_cleaned)

# Сохранение витрины
save_dataset_to_s3(bi_dataset, bucket_name, output_path)

# Закрытие сессии Spark
spark.stop()
