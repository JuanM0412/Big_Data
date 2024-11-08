spark

sc

from pyspark.sql import SparkSession

spark.sql("SELECT * FROM hdi")

spark.sql("SELECT country, gni FROM hdi WHERE hdi > 2000")

spark.sql("SELECT h.country, gni, expct FROM hdi h JOIN EXPO e ON (h.country = e.country) WHERE gni > 2000")

spark.sql("SELECT word, count(1) AS count FROM (SELECT explode(split(line,' ')) AS word FROM docs) w \
            GROUP BY word \
            ORDER BY count DESC LIMIT 10;")
