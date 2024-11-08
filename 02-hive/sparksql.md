```pyspark
spark
```

    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>User</th><th>Current session?</th></tr><tr><td>6</td><td>application_1731028054550_0007</td><td>pyspark</td><td>idle</td><td><a target="_blank" href="http://ip-172-31-37-68.ec2.internal:20888/proxy/application_1731028054550_0007/">Link</a></td><td><a target="_blank" href="http://ip-172-31-47-124.ec2.internal:8042/node/containerlogs/container_1731028054550_0007_01_000001/livy">Link</a></td><td>None</td><td>✔</td></tr></table>



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    SparkSession available as 'spark'.



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    <pyspark.sql.session.SparkSession object at 0x7f86ac2c4e20>


```pyspark
sc
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    <SparkContext master=yarn appName=livy-session-6>


```pyspark
from pyspark.sql import SparkSession
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
spark.sql("SELECT * FROM hdi")
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    DataFrame[id: int, country: string, hdi: float, lifeex: int, mysch: int, eysch: int, gni: int]


```pyspark
spark.sql("SELECT country, gni FROM hdi WHERE hdi > 2000")
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    DataFrame[country: string, gni: int]


```pyspark
spark.sql("SELECT h.country, gni, expct FROM hdi h JOIN EXPO e ON (h.country = e.country) WHERE gni > 2000")
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    DataFrame[country: string, gni: int, expct: float]


```pyspark
spark.sql("SELECT word, count(1) AS count FROM (SELECT explode(split(line,' ')) AS word FROM docs) w \
            GROUP BY word \
            ORDER BY count DESC LIMIT 10;")
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    DataFrame[word: string, count: bigint]
