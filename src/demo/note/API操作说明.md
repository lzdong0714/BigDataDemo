## Spark 2.X 的API常用示例以及说明

##### CSV文件读取

``` scala
    val sparkSession = SparkSession.builder().appName("instrument").getOrCreate()
    val sqlContext = sparkSession.sqlContext
    val dataFrame =
      sqlContext.read.format("com.databricks.spark.csv")
          .option("header","true")  // 导入表头
          .option("delimiter",",")  // 默认分隔符
          .option("inferSchema","true") //自动识别表结构
          .load(inputPath)
```

``` sql
# spark-sql 中按天连续的sql查询 

WITH s AS (
   SELECT
    stationName,
    date,
    date_add(date, -(row_number() over (partition by stationName order by date)) + 1) as discriminator
  FROM stations
)
SELECT
  stationName,
  MIN(date) as start,
  COUNT(1) AS duration
FROM s GROUP BY stationName, discriminator
```

