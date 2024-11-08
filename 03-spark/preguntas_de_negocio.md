```pyspark
spark
```


```pyspark
sc
```


```pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count
```


```pyspark
df = spark.read.csv("s3://big-data-topicos/bigdata/datasets/covid19/Casos_positivos_de_COVID-19_en_Colombia-100K.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("covid_data")
```


```pyspark
# Los 10 departamentos con más casos de COVID en Colombia
```


```pyspark
# DataFrames
top_departments_df = df.groupBy("departamento") \
    .agg(count("id_caso").alias("total_casos")) \
    .orderBy(desc("total_casos")) \
    .limit(10)
top_departments_df.show()
```


```pyspark
# SparkSQL
spark.sql("""
    SELECT departamento, COUNT(id_caso) AS total_casos
    FROM covid_data
    GROUP BY departamento
    ORDER BY total_casos DESC
    LIMIT 10
""").show()
```


```pyspark
# Las 10 ciudades con más casos de COVID en Colombia
```


```pyspark
# DataFrame
top_cities_df = df.groupBy("municipio") \
    .agg(count("id_caso").alias("total_casos")) \
    .orderBy(desc("total_casos")) \
    .limit(10)
top_cities_df.show()
```


```pyspark
# SparkSQL
spark.sql("""
    SELECT municipio, COUNT(id_caso) AS total_casos
    FROM covid_data
    GROUP BY municipio
    ORDER BY total_casos DESC
    LIMIT 10
""").show()
```


```pyspark
# Los 10 días con más casos de COVID en Colombia
```


```pyspark
# DataFrames
top_days_df = df.groupBy("fecha_reporte_web") \
    .agg(count("id_caso").alias("total_casos")) \
    .orderBy(desc("total_casos")) \
    .limit(10)
top_days_df.show()
```


```pyspark
# SparkSQL
spark.sql("""
    SELECT fecha_reporte_web, COUNT(id_caso) AS total_casos
    FROM covid_data
    GROUP BY fecha_reporte_web
    ORDER BY total_casos DESC
    LIMIT 10
""").show()
```


```pyspark
# Distribución de casos por edades
```


```pyspark
# DataFrames
age_distribution_df = df.groupBy("edad") \
    .agg(count("id_caso").alias("total_casos")) \
    .orderBy("edad")
age_distribution_df.show()
```


```pyspark
# SparkSQL
spark.sql("""
    SELECT edad, COUNT(id_caso) AS total_casos
    FROM covid_data
    GROUP BY edad
    ORDER BY edad
""").show()
```


```pyspark
# ¿Cuántos casos hay por cada género en Colombia?
```


```pyspark
# DataFrame
gender_distribution_df = df.groupBy("sexo") \
    .agg(count("id_caso").alias("total_casos")) \
    .orderBy(desc("total_casos"))
gender_distribution_df.show()
```


```pyspark
# SparkSQL
spark.sql("""
    SELECT sexo, COUNT(id_caso) AS total_casos
    FROM covid_data
    GROUP BY sexo
    ORDER BY total_casos DESC
""").show()
```


```pyspark
# Guardar los datos
```


```pyspark
write_uri='s3a://big-data-topicos/bigdata/output/business/csv'
```


```pyspark
df.coalesce(1).write.format("csv").option("header", "true").save(write_uri)
```


```pyspark
parquet_uri='s3a://big-data-topicos/bigdata/output/business/parquet'
```


```pyspark
df.write.format('parquet').save(parquet_uri)
```
