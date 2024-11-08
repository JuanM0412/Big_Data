spark

sc

# Load csv Dataset
df = spark.read.csv('s3://big-data-topicos/bigdata/datasets/covid19/Casos_positivos_de_COVID-19_en_Colombia-100K.csv', inferSchema=True, header=True)

#columns of dataframe
df.columns

#check number of columns
len(df.columns)

#number of records in dataframe
df.count()

#shape of dataset
print((df.count(), len(df.columns)))

#printSchema
df.printSchema()

#fisrt few rows of dataframe
df.show()

#select only 2 columns
df.select('edad', 'sexo').show(5)

#info about dataframe
df.describe().show()

from pyspark.sql.functions import when

# Create Column
df = df.withColumn("adulto_mayor", when(df["edad"] >= 60, False).otherwise(True))
df.show()

# Delete Column
df = df.drop("adulto_mayor")
df.show()

#filter the records
df.filter(df['departamento'] =='ANTIOQUIA').show()

#filter the records
df.filter(df['departamento'] =='ANTIOQUIA').select('tipo_contagio', 'estado', 'tipo_recuperacion').show(10)

#filter the multiple conditions
df.filter((df['departamento'] =='ANTIOQUIA') & (df['edad'] > 40)).show()

#Distinct Values in a column
df.select('codigo_iso_pais').distinct().show(10)

#distinct value count
df.select('codigo_iso_pais').distinct().count()

df.groupBy('codigo_iso_pais').count().show(5, False)

# Value counts
df.groupBy('codigo_iso_pais').count().orderBy('count', ascending=True).show(5, False)

# Value counts
df.groupBy('codigo_iso_pais').mean().show(5, False)

df.groupBy('codigo_iso_pais').sum().show(5, False)

# Value counts
df.groupBy('codigo_iso_pais').max().show(5, False)

# Value counts
df.groupBy('codigo_iso_pais').min().show(5, False)

#Aggregation
df.groupBy('codigo_iso_pais').agg({'edad': 'sum'}).show(5, False)

# UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

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

df = df.withColumn("categoria_edad", clasificar_edad_udf(df.edad))

df.show()

# Lambda
mayor_de_edad_lambda = udf(lambda edad: "Sí" if edad >= 18 else "No", StringType())

df = df.withColumn("mayor_de_edad", mayor_de_edad_lambda(df.edad))

df.show()

# saving file (csv)

#target directory
write_uri='s3a://big-data-topicos/bigdata/output/jupyter/csv'

#save the dataframe as single csv
df.coalesce(1).write.format("csv").option("header", "true").save(write_uri)

# parquet

#target location
parquet_uri='s3a://big-data-topicos/bigdata/output/jupyter/parquet'

#save the data into parquet format
df.write.format('parquet').save(parquet_uri)
