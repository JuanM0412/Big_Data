# Covid19 Data Processing using Pyspark


```pyspark
spark
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    <pyspark.sql.session.SparkSession object at 0x7f537b46ef10>


```pyspark
sc
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    <SparkContext master=yarn appName=livy-session-2>


```pyspark
# Load csv Dataset
df = spark.read.csv('s3://big-data-topicos/bigdata/datasets/covid19/Casos_positivos_de_COVID-19_en_Colombia-100K.csv', inferSchema=True, header=True)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
#columns of dataframe
df.columns
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    ['fecha_reporte_web', 'id_caso', 'fecha_notificación', 'codigo_divipola_departamento', 'departamento', 'codigo_divipola_municipio', 'municipio', 'edad', 'unidad_medida_edad', 'sexo', 'tipo_contagio', 'ubicacion_caso', 'estado', 'codigo_iso_pais', 'pais', 'recuperado', 'fecha_inicio_sintomas', 'fecha_muerte', 'fecha_diagnostico', 'fecha_recuperacion', 'tipo_recuperacion', 'pertenencia_etnica', 'nombre_grupo_etnico']


```pyspark
#check number of columns
len(df.columns)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    23


```pyspark
#number of records in dataframe
df.count()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    100000


```pyspark
#shape of dataset
print((df.count(), len(df.columns)))
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    (100000, 23)


```pyspark
#printSchema
df.printSchema()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    root
     |-- fecha_reporte_web: string (nullable = true)
     |-- id_caso: integer (nullable = true)
     |-- fecha_notificación: string (nullable = true)
     |-- codigo_divipola_departamento: integer (nullable = true)
     |-- departamento: string (nullable = true)
     |-- codigo_divipola_municipio: integer (nullable = true)
     |-- municipio: string (nullable = true)
     |-- edad: integer (nullable = true)
     |-- unidad_medida_edad: integer (nullable = true)
     |-- sexo: string (nullable = true)
     |-- tipo_contagio: string (nullable = true)
     |-- ubicacion_caso: string (nullable = true)
     |-- estado: string (nullable = true)
     |-- codigo_iso_pais: integer (nullable = true)
     |-- pais: string (nullable = true)
     |-- recuperado: string (nullable = true)
     |-- fecha_inicio_sintomas: string (nullable = true)
     |-- fecha_muerte: string (nullable = true)
     |-- fecha_diagnostico: string (nullable = true)
     |-- fecha_recuperacion: string (nullable = true)
     |-- tipo_recuperacion: string (nullable = true)
     |-- pertenencia_etnica: integer (nullable = true)
     |-- nombre_grupo_etnico: string (nullable = true)


```pyspark
#fisrt few rows of dataframe
df.show()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+
    |fecha_reporte_web|id_caso|fecha_notificación|codigo_divipola_departamento|departamento|codigo_divipola_municipio|    municipio|edad|unidad_medida_edad|sexo|tipo_contagio|ubicacion_caso|estado|codigo_iso_pais|                pais|recuperado|fecha_inicio_sintomas|fecha_muerte|fecha_diagnostico|fecha_recuperacion|tipo_recuperacion|pertenencia_etnica|nombre_grupo_etnico|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+
    | 6/3/2020 0:00:00|      1|  2/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  19|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|    27/2/2020 0:00:00|        NULL| 6/3/2020 0:00:00| 13/3/2020 0:00:00|              PCR|                 6|               NULL|
    | 9/3/2020 0:00:00|      2|  6/3/2020 0:00:00|                          76|       VALLE|                    76111|         BUGA|  34|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     4/3/2020 0:00:00|        NULL| 9/3/2020 0:00:00| 19/3/2020 0:00:00|              PCR|                 5|               NULL|
    | 9/3/2020 0:00:00|      3|  7/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  50|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    29/2/2020 0:00:00|        NULL| 9/3/2020 0:00:00| 15/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      4|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  55|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      5|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  25|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     8/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      6| 10/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5360|       ITAGUI|  27|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      7|  8/3/2020 0:00:00|                       13001|   CARTAGENA|                    13001|    CARTAGENA|  85|                 1|   F|    Importado|          Casa|  Leve|            840|ESTADOS UNIDOS DE...|Recuperado|     2/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 17/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      8|  9/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  22|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      9|  8/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  28|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|
    |12/3/2020 0:00:00|     10| 12/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  36|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 6|               NULL|
    |12/3/2020 0:00:00|     11| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  42|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 31/3/2020 0:00:00|              PCR|                 6|               NULL|
    |12/3/2020 0:00:00|     12| 10/3/2020 0:00:00|                          41|       HUILA|                    41001|        NEIVA|  74|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00|  9/4/2020 0:00:00|              PCR|                 6|               NULL|
    |12/3/2020 0:00:00|     13| 10/3/2020 0:00:00|                          41|       HUILA|                    41001|        NEIVA|  68|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 30/3/2020 0:00:00|              PCR|                 6|               NULL|
    |13/3/2020 0:00:00|     14| 10/3/2020 0:00:00|                          76|       VALLE|                    76520|      PALMIRA|  48|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 5|               NULL|
    |13/3/2020 0:00:00|     15| 13/3/2020 0:00:00|                          50|        META|                    50001|VILLAVICENCIO|  30|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     9/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|
    |13/3/2020 0:00:00|     16| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  61|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|     8/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 5|               NULL|
    |14/3/2020 0:00:00|     17| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  73|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    28/2/2020 0:00:00|        NULL|14/3/2020 0:00:00| 14/3/2020 0:00:00|              PCR|                 6|               NULL|
    |14/3/2020 0:00:00|     18| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  54|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|14/3/2020 0:00:00|  7/4/2020 0:00:00|           Tiempo|                 6|               NULL|
    |14/3/2020 0:00:00|     19| 12/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  54|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     9/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 24/3/2020 0:00:00|              PCR|                 6|               NULL|
    |14/3/2020 0:00:00|     20| 11/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  26|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     9/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 24/3/2020 0:00:00|              PCR|                 6|               NULL|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+
    only showing top 20 rows


```pyspark
#select only 2 columns
df.select('edad', 'sexo').show(5)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +----+----+
    |edad|sexo|
    +----+----+
    |  19|   F|
    |  34|   M|
    |  50|   F|
    |  55|   M|
    |  25|   M|
    +----+----+
    only showing top 5 rows


```pyspark
#info about dataframe
df.describe().show()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-------+-----------------+------------------+------------------+----------------------------+------------+-------------------------+---------------+------------------+-------------------+------+-------------+--------------+---------+------------------+---------+----------+---------------------+----------------+-----------------+------------------+-----------------+------------------+-------------------+
    |summary|fecha_reporte_web|           id_caso|fecha_notificación|codigo_divipola_departamento|departamento|codigo_divipola_municipio|      municipio|              edad| unidad_medida_edad|  sexo|tipo_contagio|ubicacion_caso|   estado|   codigo_iso_pais|     pais|recuperado|fecha_inicio_sintomas|    fecha_muerte|fecha_diagnostico|fecha_recuperacion|tipo_recuperacion|pertenencia_etnica|nombre_grupo_etnico|
    +-------+-----------------+------------------+------------------+----------------------------+------------+-------------------------+---------------+------------------+-------------------+------+-------------+--------------+---------+------------------+---------+----------+---------------------+----------------+-----------------+------------------+-----------------+------------------+-------------------+
    |  count|           100000|            100000|            100000|                      100000|      100000|                   100000|         100000|            100000|             100000|100000|       100000|        100000|   100000|               912|      912|    100000|                91615|            5633|            97693|             94832|            94832|            100000|               5658|
    |   mean|             NULL|       50038.74855|              NULL|                   2631.6288|        NULL|              25327.34487|           NULL|          39.30175|            1.00637|  NULL|         NULL|          NULL|     NULL| 596.3475877192982|     NULL|      NULL|                 NULL|            NULL|             NULL|              NULL|             NULL|           5.57693|               NULL|
    | stddev|             NULL|28870.559312724497|              NULL|           6172.660309006438|        NULL|       25830.580807180544|           NULL|18.420127848324004|0.08893538213232118|  NULL|         NULL|          NULL|     NULL|255.56508665655983|     NULL|      NULL|                 NULL|            NULL|             NULL|              NULL|             NULL| 1.182825331930982|               NULL|
    |    min| 1/4/2020 0:00:00|                 1|  1/4/2020 0:00:00|                           5|    AMAZONAS|                     5001|         ABREGO|                 1|                  1|     F|  Comunitaria|          Casa|Fallecido|                32| ALEMANIA| Fallecido|     1/3/2020 0:00:00|1/1/2021 0:00:00| 1/4/2020 0:00:00|  1/1/2021 0:00:00|              PCR|                 1|             AMBALO|
    |    max| 9/6/2020 0:00:00|            100040|  9/7/2020 0:00:00|                       47001|     VICHADA|                    99001|puerto COLOMBIA|               104|                  3|     M|  Relacionado|           N/A|      N/A|               862|VENEZUELA| fallecido|     9/7/2020 0:00:00|9/9/2020 0:00:00| 9/9/2020 0:00:00|  9/8/2020 0:00:00|           Tiempo|                 6|               ZENU|
    +-------+-----------------+------------------+------------------+----------------------------+------------+-------------------------+---------------+------------------+-------------------+------+-------------+--------------+---------+------------------+---------+----------+---------------------+----------------+-----------------+------------------+-----------------+------------------+-------------------+


```pyspark
from pyspark.sql.functions import when
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
# Create Column
df = df.withColumn("adulto_mayor", when(df["edad"] >= 60, False).otherwise(True))
df.show()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+------------+
    |fecha_reporte_web|id_caso|fecha_notificación|codigo_divipola_departamento|departamento|codigo_divipola_municipio|    municipio|edad|unidad_medida_edad|sexo|tipo_contagio|ubicacion_caso|estado|codigo_iso_pais|                pais|recuperado|fecha_inicio_sintomas|fecha_muerte|fecha_diagnostico|fecha_recuperacion|tipo_recuperacion|pertenencia_etnica|nombre_grupo_etnico|adulto_mayor|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+------------+
    | 6/3/2020 0:00:00|      1|  2/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  19|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|    27/2/2020 0:00:00|        NULL| 6/3/2020 0:00:00| 13/3/2020 0:00:00|              PCR|                 6|               NULL|        true|
    | 9/3/2020 0:00:00|      2|  6/3/2020 0:00:00|                          76|       VALLE|                    76111|         BUGA|  34|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     4/3/2020 0:00:00|        NULL| 9/3/2020 0:00:00| 19/3/2020 0:00:00|              PCR|                 5|               NULL|        true|
    | 9/3/2020 0:00:00|      3|  7/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  50|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    29/2/2020 0:00:00|        NULL| 9/3/2020 0:00:00| 15/3/2020 0:00:00|              PCR|                 6|               NULL|        true|
    |11/3/2020 0:00:00|      4|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  55|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|        true|
    |11/3/2020 0:00:00|      5|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  25|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     8/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|        true|
    |11/3/2020 0:00:00|      6| 10/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5360|       ITAGUI|  27|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|        true|
    |11/3/2020 0:00:00|      7|  8/3/2020 0:00:00|                       13001|   CARTAGENA|                    13001|    CARTAGENA|  85|                 1|   F|    Importado|          Casa|  Leve|            840|ESTADOS UNIDOS DE...|Recuperado|     2/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 17/3/2020 0:00:00|              PCR|                 6|               NULL|       false|
    |11/3/2020 0:00:00|      8|  9/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  22|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 6|               NULL|        true|
    |11/3/2020 0:00:00|      9|  8/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  28|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|        true|
    |12/3/2020 0:00:00|     10| 12/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  36|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 6|               NULL|        true|
    |12/3/2020 0:00:00|     11| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  42|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 31/3/2020 0:00:00|              PCR|                 6|               NULL|        true|
    |12/3/2020 0:00:00|     12| 10/3/2020 0:00:00|                          41|       HUILA|                    41001|        NEIVA|  74|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00|  9/4/2020 0:00:00|              PCR|                 6|               NULL|       false|
    |12/3/2020 0:00:00|     13| 10/3/2020 0:00:00|                          41|       HUILA|                    41001|        NEIVA|  68|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 30/3/2020 0:00:00|              PCR|                 6|               NULL|       false|
    |13/3/2020 0:00:00|     14| 10/3/2020 0:00:00|                          76|       VALLE|                    76520|      PALMIRA|  48|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 5|               NULL|        true|
    |13/3/2020 0:00:00|     15| 13/3/2020 0:00:00|                          50|        META|                    50001|VILLAVICENCIO|  30|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     9/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|        true|
    |13/3/2020 0:00:00|     16| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  61|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|     8/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 5|               NULL|       false|
    |14/3/2020 0:00:00|     17| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  73|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    28/2/2020 0:00:00|        NULL|14/3/2020 0:00:00| 14/3/2020 0:00:00|              PCR|                 6|               NULL|       false|
    |14/3/2020 0:00:00|     18| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  54|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|14/3/2020 0:00:00|  7/4/2020 0:00:00|           Tiempo|                 6|               NULL|        true|
    |14/3/2020 0:00:00|     19| 12/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  54|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     9/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 24/3/2020 0:00:00|              PCR|                 6|               NULL|        true|
    |14/3/2020 0:00:00|     20| 11/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  26|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     9/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 24/3/2020 0:00:00|              PCR|                 6|               NULL|        true|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+------------+
    only showing top 20 rows


```pyspark
# Delete Column
df = df.drop("adulto_mayor")
df.show()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+
    |fecha_reporte_web|id_caso|fecha_notificación|codigo_divipola_departamento|departamento|codigo_divipola_municipio|    municipio|edad|unidad_medida_edad|sexo|tipo_contagio|ubicacion_caso|estado|codigo_iso_pais|                pais|recuperado|fecha_inicio_sintomas|fecha_muerte|fecha_diagnostico|fecha_recuperacion|tipo_recuperacion|pertenencia_etnica|nombre_grupo_etnico|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+
    | 6/3/2020 0:00:00|      1|  2/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  19|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|    27/2/2020 0:00:00|        NULL| 6/3/2020 0:00:00| 13/3/2020 0:00:00|              PCR|                 6|               NULL|
    | 9/3/2020 0:00:00|      2|  6/3/2020 0:00:00|                          76|       VALLE|                    76111|         BUGA|  34|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     4/3/2020 0:00:00|        NULL| 9/3/2020 0:00:00| 19/3/2020 0:00:00|              PCR|                 5|               NULL|
    | 9/3/2020 0:00:00|      3|  7/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  50|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    29/2/2020 0:00:00|        NULL| 9/3/2020 0:00:00| 15/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      4|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  55|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      5|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  25|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     8/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      6| 10/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5360|       ITAGUI|  27|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      7|  8/3/2020 0:00:00|                       13001|   CARTAGENA|                    13001|    CARTAGENA|  85|                 1|   F|    Importado|          Casa|  Leve|            840|ESTADOS UNIDOS DE...|Recuperado|     2/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 17/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      8|  9/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  22|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      9|  8/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  28|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|
    |12/3/2020 0:00:00|     10| 12/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  36|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 6|               NULL|
    |12/3/2020 0:00:00|     11| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  42|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 31/3/2020 0:00:00|              PCR|                 6|               NULL|
    |12/3/2020 0:00:00|     12| 10/3/2020 0:00:00|                          41|       HUILA|                    41001|        NEIVA|  74|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00|  9/4/2020 0:00:00|              PCR|                 6|               NULL|
    |12/3/2020 0:00:00|     13| 10/3/2020 0:00:00|                          41|       HUILA|                    41001|        NEIVA|  68|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 30/3/2020 0:00:00|              PCR|                 6|               NULL|
    |13/3/2020 0:00:00|     14| 10/3/2020 0:00:00|                          76|       VALLE|                    76520|      PALMIRA|  48|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 5|               NULL|
    |13/3/2020 0:00:00|     15| 13/3/2020 0:00:00|                          50|        META|                    50001|VILLAVICENCIO|  30|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     9/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|
    |13/3/2020 0:00:00|     16| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  61|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|     8/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 5|               NULL|
    |14/3/2020 0:00:00|     17| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  73|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    28/2/2020 0:00:00|        NULL|14/3/2020 0:00:00| 14/3/2020 0:00:00|              PCR|                 6|               NULL|
    |14/3/2020 0:00:00|     18| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  54|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|14/3/2020 0:00:00|  7/4/2020 0:00:00|           Tiempo|                 6|               NULL|
    |14/3/2020 0:00:00|     19| 12/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  54|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     9/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 24/3/2020 0:00:00|              PCR|                 6|               NULL|
    |14/3/2020 0:00:00|     20| 11/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  26|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     9/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 24/3/2020 0:00:00|              PCR|                 6|               NULL|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+
    only showing top 20 rows


```pyspark
#filter the records
df.filter(df['departamento'] =='ANTIOQUIA').show()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-----------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+
    |fecha_reporte_web|id_caso|fecha_notificación|codigo_divipola_departamento|departamento|codigo_divipola_municipio|  municipio|edad|unidad_medida_edad|sexo|tipo_contagio|ubicacion_caso|estado|codigo_iso_pais|                pais|recuperado|fecha_inicio_sintomas|fecha_muerte|fecha_diagnostico|fecha_recuperacion|tipo_recuperacion|pertenencia_etnica|nombre_grupo_etnico|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-----------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+
    | 9/3/2020 0:00:00|      3|  7/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  50|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    29/2/2020 0:00:00|        NULL| 9/3/2020 0:00:00| 15/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      4|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  55|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      5|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  25|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     8/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      6| 10/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5360|     ITAGUI|  27|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|
    |14/3/2020 0:00:00|     20| 11/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  26|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     9/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 24/3/2020 0:00:00|              PCR|                 6|               NULL|
    |14/3/2020 0:00:00|     21| 11/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  28|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|    10/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 10/4/2020 0:00:00|              PCR|                 6|               NULL|
    |14/3/2020 0:00:00|     22| 12/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5615|   RIONEGRO|  36|                 1|   M|    Importado|          Casa|  Leve|            840|ESTADOS UNIDOS DE...|Recuperado|     9/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 24/3/2020 0:00:00|              PCR|                 6|               NULL|
    |15/3/2020 0:00:00|     32| 11/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  55|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    10/3/2020 0:00:00|        NULL|15/3/2020 0:00:00| 25/3/2020 0:00:00|              PCR|                 6|               NULL|
    |19/3/2020 0:00:00|    106| 19/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  44|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    13/3/2020 0:00:00|        NULL|19/3/2020 0:00:00| 28/3/2020 0:00:00|              PCR|                 6|               NULL|
    |19/3/2020 0:00:00|    107| 12/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  56|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    10/3/2020 0:00:00|        NULL|19/3/2020 0:00:00| 30/3/2020 0:00:00|              PCR|                 6|               NULL|
    |19/3/2020 0:00:00|    108| 17/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  57|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    14/3/2020 0:00:00|        NULL|19/3/2020 0:00:00| 28/3/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    131| 15/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  22|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    13/3/2020 0:00:00|        NULL|20/3/2020 0:00:00| 28/3/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    133| 16/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5615|   RIONEGRO|  51|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|    10/3/2020 0:00:00|        NULL|20/3/2020 0:00:00| 25/3/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    134| 17/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5380|LA ESTRELLA|  28|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|                 NULL|        NULL|20/3/2020 0:00:00|  3/4/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    135| 17/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  44|                 1|   F|    Importado|          Casa|  Leve|            276|            ALEMANIA|Recuperado|    16/3/2020 0:00:00|        NULL|20/3/2020 0:00:00|  1/4/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    136| 17/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  37|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|    13/3/2020 0:00:00|        NULL|20/3/2020 0:00:00| 13/4/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    137| 17/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5266|   ENVIGADO|  54|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|                 NULL|        NULL|20/3/2020 0:00:00|  3/4/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    141| 17/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  62|                 1|   F|    Importado|          Casa|  Leve|            191|             CROACIA|Recuperado|    10/3/2020 0:00:00|        NULL|20/3/2020 0:00:00| 25/3/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    142| 20/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|   MEDELLIN|  35|                 1|   F|    Importado|          Casa|  Leve|            591|              PANAMA|Recuperado|    14/3/2020 0:00:00|        NULL|20/3/2020 0:00:00| 29/3/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    143| 14/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5266|   ENVIGADO|  46|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    11/3/2020 0:00:00|        NULL|20/3/2020 0:00:00|  1/8/2020 0:00:00|              PCR|                 6|               NULL|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-----------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+
    only showing top 20 rows


```pyspark
#filter the records
df.filter(df['departamento'] =='ANTIOQUIA').select('tipo_contagio', 'estado', 'tipo_recuperacion').show(10)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-------------+------+-----------------+
    |tipo_contagio|estado|tipo_recuperacion|
    +-------------+------+-----------------+
    |    Importado|  Leve|              PCR|
    |  Relacionado|  Leve|              PCR|
    |  Relacionado|  Leve|              PCR|
    |  Relacionado|  Leve|              PCR|
    |  Relacionado|  Leve|              PCR|
    |  Relacionado|  Leve|              PCR|
    |    Importado|  Leve|              PCR|
    |    Importado|  Leve|              PCR|
    |    Importado|  Leve|              PCR|
    |    Importado|  Leve|              PCR|
    +-------------+------+-----------------+
    only showing top 10 rows


```pyspark
#filter the multiple conditions
df.filter((df['departamento'] =='ANTIOQUIA') & (df['edad'] > 40)).show()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-----------------+-------+------------------+----------------------------+------------+-------------------------+---------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+
    |fecha_reporte_web|id_caso|fecha_notificación|codigo_divipola_departamento|departamento|codigo_divipola_municipio|municipio|edad|unidad_medida_edad|sexo|tipo_contagio|ubicacion_caso|estado|codigo_iso_pais|                pais|recuperado|fecha_inicio_sintomas|fecha_muerte|fecha_diagnostico|fecha_recuperacion|tipo_recuperacion|pertenencia_etnica|nombre_grupo_etnico|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+---------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+
    | 9/3/2020 0:00:00|      3|  7/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  50|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    29/2/2020 0:00:00|        NULL| 9/3/2020 0:00:00| 15/3/2020 0:00:00|              PCR|                 6|               NULL|
    |11/3/2020 0:00:00|      4|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  55|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|
    |15/3/2020 0:00:00|     32| 11/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  55|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    10/3/2020 0:00:00|        NULL|15/3/2020 0:00:00| 25/3/2020 0:00:00|              PCR|                 6|               NULL|
    |19/3/2020 0:00:00|    106| 19/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  44|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    13/3/2020 0:00:00|        NULL|19/3/2020 0:00:00| 28/3/2020 0:00:00|              PCR|                 6|               NULL|
    |19/3/2020 0:00:00|    107| 12/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  56|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    10/3/2020 0:00:00|        NULL|19/3/2020 0:00:00| 30/3/2020 0:00:00|              PCR|                 6|               NULL|
    |19/3/2020 0:00:00|    108| 17/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  57|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    14/3/2020 0:00:00|        NULL|19/3/2020 0:00:00| 28/3/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    133| 16/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5615| RIONEGRO|  51|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|    10/3/2020 0:00:00|        NULL|20/3/2020 0:00:00| 25/3/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    135| 17/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  44|                 1|   F|    Importado|          Casa|  Leve|            276|            ALEMANIA|Recuperado|    16/3/2020 0:00:00|        NULL|20/3/2020 0:00:00|  1/4/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    137| 17/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5266| ENVIGADO|  54|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|                 NULL|        NULL|20/3/2020 0:00:00|  3/4/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    141| 17/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  62|                 1|   F|    Importado|          Casa|  Leve|            191|             CROACIA|Recuperado|    10/3/2020 0:00:00|        NULL|20/3/2020 0:00:00| 25/3/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    143| 14/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5266| ENVIGADO|  46|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    11/3/2020 0:00:00|        NULL|20/3/2020 0:00:00|  1/8/2020 0:00:00|              PCR|                 6|               NULL|
    |20/3/2020 0:00:00|    144| 20/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  41|                 1|   M|    Importado|          Casa|  Leve|            591|              PANAMA|Recuperado|    14/3/2020 0:00:00|        NULL|20/3/2020 0:00:00|  5/4/2020 0:00:00|              PCR|                 6|               NULL|
    |22/3/2020 0:00:00|    236| 19/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5321|  GUATAPE|  52|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|    10/3/2020 0:00:00|        NULL|22/3/2020 0:00:00| 25/3/2020 0:00:00|              PCR|                 6|               NULL|
    |22/3/2020 0:00:00|    238| 16/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  61|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    14/3/2020 0:00:00|        NULL|22/3/2020 0:00:00| 11/4/2020 0:00:00|           Tiempo|                 6|               NULL|
    |22/3/2020 0:00:00|    239| 16/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5615| RIONEGRO|  41|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    14/3/2020 0:00:00|        NULL|22/3/2020 0:00:00| 29/3/2020 0:00:00|              PCR|                 6|               NULL|
    |22/3/2020 0:00:00|    240| 18/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5088|    BELLO|  41|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     9/3/2020 0:00:00|        NULL|22/3/2020 0:00:00| 25/4/2020 0:00:00|              PCR|                 6|               NULL|
    |23/3/2020 0:00:00|    263| 21/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  59|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    16/3/2020 0:00:00|        NULL|23/3/2020 0:00:00| 30/3/2020 0:00:00|              PCR|                 6|               NULL|
    |23/3/2020 0:00:00|    272| 18/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  47|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    14/3/2020 0:00:00|        NULL|23/3/2020 0:00:00| 29/3/2020 0:00:00|              PCR|                 6|               NULL|
    |23/3/2020 0:00:00|    273| 19/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  87|                 1|   M|    Importado|          Casa|  Leve|            591|              PANAMA|Recuperado|    11/3/2020 0:00:00|        NULL|23/3/2020 0:00:00| 19/4/2020 0:00:00|              PCR|                 6|               NULL|
    |23/3/2020 0:00:00|    292| 19/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001| MEDELLIN|  43|                 1|   F|    Importado|          Casa|  Leve|            840|ESTADOS UNIDOS DE...|Recuperado|    15/3/2020 0:00:00|        NULL|23/3/2020 0:00:00| 29/3/2020 0:00:00|              PCR|                 6|               NULL|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+---------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+
    only showing top 20 rows


```pyspark
#Distinct Values in a column
df.select('codigo_iso_pais').distinct().show(10)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +---------------+
    |codigo_iso_pais|
    +---------------+
    |           NULL|
    |            756|
    |            300|
    |             76|
    |            192|
    |            250|
    |            840|
    |            191|
    |            484|
    |            531|
    +---------------+
    only showing top 10 rows


```pyspark
#distinct value count
df.select('codigo_iso_pais').distinct().count()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    44


```pyspark
df.groupBy('codigo_iso_pais').count().show(5, False)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +---------------+-----+
    |codigo_iso_pais|count|
    +---------------+-----+
    |NULL           |99088|
    |756            |1    |
    |784            |1    |
    |792            |27   |
    |818            |11   |
    +---------------+-----+
    only showing top 5 rows


```pyspark
# Value counts
df.groupBy('codigo_iso_pais').count().orderBy('count', ascending=True).show(5, False)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +---------------+-----+
    |codigo_iso_pais|count|
    +---------------+-----+
    |340            |1    |
    |320            |1    |
    |56             |1    |
    |36             |1    |
    |858            |1    |
    +---------------+-----+
    only showing top 5 rows


```pyspark
# Value counts
df.groupBy('codigo_iso_pais').mean().show(5, False)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +---------------+-----------------+---------------------------------+------------------------------+-----------------+-----------------------+--------------------+-----------------------+
    |codigo_iso_pais|avg(id_caso)     |avg(codigo_divipola_departamento)|avg(codigo_divipola_municipio)|avg(edad)        |avg(unidad_medida_edad)|avg(codigo_iso_pais)|avg(pertenencia_etnica)|
    +---------------+-----------------+---------------------------------+------------------------------+-----------------+-----------------------+--------------------+-----------------------+
    |NULL           |50456.31193484579|2650.1501897303406               |25284.244388826093            |39.27429153883417|1.006428629097368      |NULL                |5.574933392539965      |
    |756            |962.0            |76.0                             |76001.0                       |68.0             |1.0                    |756.0               |6.0                    |
    |784            |620.0            |76.0                             |76520.0                       |45.0             |1.0                    |784.0               |6.0                    |
    |792            |1132.037037037037|312.8888888888889                |16991.0                       |45.74074074074074|1.0                    |792.0               |5.814814814814815      |
    |818            |2227.909090909091|12.272727272727273               |12316.636363636364            |55.45454545454545|1.0                    |818.0               |5.909090909090909      |
    +---------------+-----------------+---------------------------------+------------------------------+-----------------+-----------------------+--------------------+-----------------------+
    only showing top 5 rows


```pyspark
df.groupBy('codigo_iso_pais').sum().show(5, False)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +---------------+------------+---------------------------------+------------------------------+---------+-----------------------+--------------------+-----------------------+
    |codigo_iso_pais|sum(id_caso)|sum(codigo_divipola_departamento)|sum(codigo_divipola_municipio)|sum(edad)|sum(unidad_medida_edad)|sum(codigo_iso_pais)|sum(pertenencia_etnica)|
    +---------------+------------+---------------------------------+------------------------------+---------+-----------------------+--------------------+-----------------------+
    |NULL           |4999615037  |262598082                        |2505365208                    |3891611  |99725                  |NULL                |552409                 |
    |756            |962         |76                               |76001                         |68       |1                      |756                 |6                      |
    |784            |620         |76                               |76520                         |45       |1                      |784                 |6                      |
    |792            |30565       |8448                             |458757                        |1235     |27                     |21384               |157                    |
    |818            |24507       |135                              |135483                        |610      |11                     |8998                |65                     |
    +---------------+------------+---------------------------------+------------------------------+---------+-----------------------+--------------------+-----------------------+
    only showing top 5 rows


```pyspark
# Value counts
df.groupBy('codigo_iso_pais').max().show(5, False)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +---------------+------------+---------------------------------+------------------------------+---------+-----------------------+--------------------+-----------------------+
    |codigo_iso_pais|max(id_caso)|max(codigo_divipola_departamento)|max(codigo_divipola_municipio)|max(edad)|max(unidad_medida_edad)|max(codigo_iso_pais)|max(pertenencia_etnica)|
    +---------------+------------+---------------------------------+------------------------------+---------+-----------------------+--------------------+-----------------------+
    |NULL           |100040      |47001                            |99001                         |104      |3                      |NULL                |6                      |
    |756            |962         |76                               |76001                         |68       |1                      |756                 |6                      |
    |784            |620         |76                               |76520                         |45       |1                      |784                 |6                      |
    |792            |3743        |8001                             |76828                         |77       |1                      |792                 |6                      |
    |818            |4713        |25                               |25473                         |75       |1                      |818                 |6                      |
    +---------------+------------+---------------------------------+------------------------------+---------+-----------------------+--------------------+-----------------------+
    only showing top 5 rows


```pyspark
# Value counts
df.groupBy('codigo_iso_pais').min().show(5, False)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +---------------+------------+---------------------------------+------------------------------+---------+-----------------------+--------------------+-----------------------+
    |codigo_iso_pais|min(id_caso)|min(codigo_divipola_departamento)|min(codigo_divipola_municipio)|min(edad)|min(unidad_medida_edad)|min(codigo_iso_pais)|min(pertenencia_etnica)|
    +---------------+------------+---------------------------------+------------------------------+---------+-----------------------+--------------------+-----------------------+
    |NULL           |4           |5                                |5001                          |1        |1                      |NULL                |1                      |
    |756            |962         |76                               |76001                         |68       |1                      |756                 |6                      |
    |784            |620         |76                               |76520                         |45       |1                      |784                 |6                      |
    |792            |70          |5                                |5001                          |22       |1                      |792                 |1                      |
    |818            |540         |11                               |11001                         |36       |1                      |818                 |5                      |
    +---------------+------------+---------------------------------+------------------------------+---------+-----------------------+--------------------+-----------------------+
    only showing top 5 rows


```pyspark
#Aggregation
df.groupBy('codigo_iso_pais').agg({'edad': 'sum'}).show(5, False)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +---------------+---------+
    |codigo_iso_pais|sum(edad)|
    +---------------+---------+
    |NULL           |3891611  |
    |756            |68       |
    |784            |45       |
    |792            |1235     |
    |818            |610      |
    +---------------+---------+
    only showing top 5 rows


```pyspark
# UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
def clasificar_edad(edad):
    if edad is None:
        return "Desconocido"
    elif edad < 18:
        return "Niño"
    elif 18 <= edad < 60:
        return "Adulto"
    else:
        return "Adulto Mayor"

clasificar_edad_udf = udf(clasificar_edad, StringType())
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
df = df.withColumn("categoria_edad", clasificar_edad_udf(df.edad))
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
df.show()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+--------------+
    |fecha_reporte_web|id_caso|fecha_notificación|codigo_divipola_departamento|departamento|codigo_divipola_municipio|    municipio|edad|unidad_medida_edad|sexo|tipo_contagio|ubicacion_caso|estado|codigo_iso_pais|                pais|recuperado|fecha_inicio_sintomas|fecha_muerte|fecha_diagnostico|fecha_recuperacion|tipo_recuperacion|pertenencia_etnica|nombre_grupo_etnico|categoria_edad|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+--------------+
    | 6/3/2020 0:00:00|      1|  2/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  19|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|    27/2/2020 0:00:00|        NULL| 6/3/2020 0:00:00| 13/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|
    | 9/3/2020 0:00:00|      2|  6/3/2020 0:00:00|                          76|       VALLE|                    76111|         BUGA|  34|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     4/3/2020 0:00:00|        NULL| 9/3/2020 0:00:00| 19/3/2020 0:00:00|              PCR|                 5|               NULL|        Adulto|
    | 9/3/2020 0:00:00|      3|  7/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  50|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    29/2/2020 0:00:00|        NULL| 9/3/2020 0:00:00| 15/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|
    |11/3/2020 0:00:00|      4|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  55|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|
    |11/3/2020 0:00:00|      5|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  25|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     8/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|
    |11/3/2020 0:00:00|      6| 10/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5360|       ITAGUI|  27|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|
    |11/3/2020 0:00:00|      7|  8/3/2020 0:00:00|                       13001|   CARTAGENA|                    13001|    CARTAGENA|  85|                 1|   F|    Importado|          Casa|  Leve|            840|ESTADOS UNIDOS DE...|Recuperado|     2/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 17/3/2020 0:00:00|              PCR|                 6|               NULL|  Adulto Mayor|
    |11/3/2020 0:00:00|      8|  9/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  22|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|
    |11/3/2020 0:00:00|      9|  8/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  28|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|
    |12/3/2020 0:00:00|     10| 12/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  36|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|
    |12/3/2020 0:00:00|     11| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  42|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 31/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|
    |12/3/2020 0:00:00|     12| 10/3/2020 0:00:00|                          41|       HUILA|                    41001|        NEIVA|  74|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00|  9/4/2020 0:00:00|              PCR|                 6|               NULL|  Adulto Mayor|
    |12/3/2020 0:00:00|     13| 10/3/2020 0:00:00|                          41|       HUILA|                    41001|        NEIVA|  68|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 30/3/2020 0:00:00|              PCR|                 6|               NULL|  Adulto Mayor|
    |13/3/2020 0:00:00|     14| 10/3/2020 0:00:00|                          76|       VALLE|                    76520|      PALMIRA|  48|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 5|               NULL|        Adulto|
    |13/3/2020 0:00:00|     15| 13/3/2020 0:00:00|                          50|        META|                    50001|VILLAVICENCIO|  30|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     9/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|
    |13/3/2020 0:00:00|     16| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  61|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|     8/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 5|               NULL|  Adulto Mayor|
    |14/3/2020 0:00:00|     17| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  73|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    28/2/2020 0:00:00|        NULL|14/3/2020 0:00:00| 14/3/2020 0:00:00|              PCR|                 6|               NULL|  Adulto Mayor|
    |14/3/2020 0:00:00|     18| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  54|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|14/3/2020 0:00:00|  7/4/2020 0:00:00|           Tiempo|                 6|               NULL|        Adulto|
    |14/3/2020 0:00:00|     19| 12/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  54|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     9/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 24/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|
    |14/3/2020 0:00:00|     20| 11/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  26|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     9/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 24/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+--------------+
    only showing top 20 rows


```pyspark
# Lambda
mayor_de_edad_lambda = udf(lambda edad: "Sí" if edad >= 18 else "No", StringType())
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
df = df.withColumn("mayor_de_edad", mayor_de_edad_lambda(df.edad))
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
df.show()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+--------------+-------------+
    |fecha_reporte_web|id_caso|fecha_notificación|codigo_divipola_departamento|departamento|codigo_divipola_municipio|    municipio|edad|unidad_medida_edad|sexo|tipo_contagio|ubicacion_caso|estado|codigo_iso_pais|                pais|recuperado|fecha_inicio_sintomas|fecha_muerte|fecha_diagnostico|fecha_recuperacion|tipo_recuperacion|pertenencia_etnica|nombre_grupo_etnico|categoria_edad|mayor_de_edad|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+--------------+-------------+
    | 6/3/2020 0:00:00|      1|  2/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  19|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|    27/2/2020 0:00:00|        NULL| 6/3/2020 0:00:00| 13/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|           S?|
    | 9/3/2020 0:00:00|      2|  6/3/2020 0:00:00|                          76|       VALLE|                    76111|         BUGA|  34|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     4/3/2020 0:00:00|        NULL| 9/3/2020 0:00:00| 19/3/2020 0:00:00|              PCR|                 5|               NULL|        Adulto|           S?|
    | 9/3/2020 0:00:00|      3|  7/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  50|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    29/2/2020 0:00:00|        NULL| 9/3/2020 0:00:00| 15/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|           S?|
    |11/3/2020 0:00:00|      4|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  55|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|           S?|
    |11/3/2020 0:00:00|      5|  9/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  25|                 1|   M|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     8/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|           S?|
    |11/3/2020 0:00:00|      6| 10/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5360|       ITAGUI|  27|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 26/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|           S?|
    |11/3/2020 0:00:00|      7|  8/3/2020 0:00:00|                       13001|   CARTAGENA|                    13001|    CARTAGENA|  85|                 1|   F|    Importado|          Casa|  Leve|            840|ESTADOS UNIDOS DE...|Recuperado|     2/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 17/3/2020 0:00:00|              PCR|                 6|               NULL|  Adulto Mayor|           S?|
    |11/3/2020 0:00:00|      8|  9/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  22|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|           S?|
    |11/3/2020 0:00:00|      9|  8/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  28|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|11/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|           S?|
    |12/3/2020 0:00:00|     10| 12/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  36|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|           S?|
    |12/3/2020 0:00:00|     11| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  42|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 31/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|           S?|
    |12/3/2020 0:00:00|     12| 10/3/2020 0:00:00|                          41|       HUILA|                    41001|        NEIVA|  74|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00|  9/4/2020 0:00:00|              PCR|                 6|               NULL|  Adulto Mayor|           S?|
    |12/3/2020 0:00:00|     13| 10/3/2020 0:00:00|                          41|       HUILA|                    41001|        NEIVA|  68|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     6/3/2020 0:00:00|        NULL|12/3/2020 0:00:00| 30/3/2020 0:00:00|              PCR|                 6|               NULL|  Adulto Mayor|           S?|
    |13/3/2020 0:00:00|     14| 10/3/2020 0:00:00|                          76|       VALLE|                    76520|      PALMIRA|  48|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 21/3/2020 0:00:00|              PCR|                 5|               NULL|        Adulto|           S?|
    |13/3/2020 0:00:00|     15| 13/3/2020 0:00:00|                          50|        META|                    50001|VILLAVICENCIO|  30|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     9/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|           S?|
    |13/3/2020 0:00:00|     16| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  61|                 1|   F|    Importado|          Casa|  Leve|            380|              ITALIA|Recuperado|     8/3/2020 0:00:00|        NULL|13/3/2020 0:00:00| 23/3/2020 0:00:00|              PCR|                 5|               NULL|  Adulto Mayor|           S?|
    |14/3/2020 0:00:00|     17| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  73|                 1|   F|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|    28/2/2020 0:00:00|        NULL|14/3/2020 0:00:00| 14/3/2020 0:00:00|              PCR|                 6|               NULL|  Adulto Mayor|           S?|
    |14/3/2020 0:00:00|     18| 11/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  54|                 1|   M|    Importado|          Casa|  Leve|            724|              ESPAÑA|Recuperado|     7/3/2020 0:00:00|        NULL|14/3/2020 0:00:00|  7/4/2020 0:00:00|           Tiempo|                 6|               NULL|        Adulto|           S?|
    |14/3/2020 0:00:00|     19| 12/3/2020 0:00:00|                          11|      BOGOTA|                    11001|       BOGOTA|  54|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     9/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 24/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|           S?|
    |14/3/2020 0:00:00|     20| 11/3/2020 0:00:00|                           5|   ANTIOQUIA|                     5001|     MEDELLIN|  26|                 1|   F|  Relacionado|          Casa|  Leve|           NULL|                NULL|Recuperado|     9/3/2020 0:00:00|        NULL|14/3/2020 0:00:00| 24/3/2020 0:00:00|              PCR|                 6|               NULL|        Adulto|           S?|
    +-----------------+-------+------------------+----------------------------+------------+-------------------------+-------------+----+------------------+----+-------------+--------------+------+---------------+--------------------+----------+---------------------+------------+-----------------+------------------+-----------------+------------------+-------------------+--------------+-------------+
    only showing top 20 rows


```pyspark
# saving file (csv)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
#target directory
write_uri='s3a://big-data-topicos/bigdata/output/jupyter/csv'
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
#save the dataframe as single csv
df.coalesce(1).write.format("csv").option("header", "true").save(write_uri)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
# parquet
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
#target location
parquet_uri='s3a://big-data-topicos/bigdata/output/jupyter/parquet'
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
#save the data into parquet format
df.write.format('parquet').save(parquet_uri)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…

