package demo.process

import java.sql.Timestamp

import com.yunheit.common.util.StringUtil
import demo.Submit.Config
import demo.util.BaseUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class DemoProcess(sc:SparkContext, config: Config) extends java.io.Serializable {
  val inputPath = BaseUtil.handle_OS_path(config.input)
  val outputPath = BaseUtil.handle_OS_path(config.output)

  def localProcess(): Unit = {
    val dataFrame = instrumentProcessMySQL()
//    instrumentProcess(dataFrame)
    instrumentProcess(dataFrame)
//    demoProcess()
//    demo_test_1()

  }


  def instrumentProcessMySQL(): DataFrame ={
    val sparkSession = SparkSession.builder().appName("instrument").getOrCreate()
    val jdbcDF = sparkSession.read.format("jdbc").option("url","jdbc:mysql://47.92.154.64:3306/hnty?autoReconnect=true&" +
      "useUnicode=true&characterEncoding=utf-8&useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false" +
      "&serverTimezone=UTC&serverTimezone=Asia/Shanghai")
      .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "work_history")//*****是表名
      .option("user", "hnty").option("password", "Hnty2018!@").load()

    val count = 2000

    jdbcDF.createTempView("work_history_tmp")
//    val data_selected = jdbcDF.select("instrument_id","report_time","arrived_time","created_time",
//    "longitude","latitude","altitude").sqlContext.sql(s"ds")

    val data_selected=sparkSession.sql(s"select instrument_id,report_time,arrived_time,created_time,longitude,latitude,altitude " +
      s"FROM work_history_tmp ORDER BY id DESC  limit ${count}")

    data_selected
  }



  def instrumentProcess(dataFrame: DataFrame):Unit={
//    val sparkSession = SparkSession.builder().appName("instrument").getOrCreate()
//    val sqlContext = sparkSession.sqlContext
//    val dataFrame =
//      sqlContext.read.format("com.databricks.spark.csv")
//          .option("header","true")  // 导入表头
//          .option("delimiter",",")  // 默认分隔符
//          .option("inferSchema","true") //自动识别表结构
//          .load(inputPath)

//    dataFrame.show(100)
    val schema = dataFrame.schema

    print(schema)
    val data_afterFilter=dataFrame
//      .filter("altitude is not null")
//      .show(50)

    val rddRaw = data_afterFilter.rdd
      var instrumentId:String = ""
      var reporterTime:Timestamp = null
      var longitude:Double = 0.0
      var latitude:Double = 0.0
      var altiude: Double = 0.0
      var workTime:Long = 0
      val interval:Long = 60
      val geoRangeInvterval = 0.0001
      val rdd_step_1 =
        rddRaw.map(row=>{
          instrumentId = row.getAs[String](0)
          reporterTime = row.getAs[Timestamp](1)
          longitude = row.getAs[Double](4)
          latitude = row.getAs[Double](5)
          altiude = row.getAs[Double](6).toDouble
          (instrumentId,(reporterTime,s"${longitude}|${latitude}|${altiude}"))
        })
        .groupByKey()
        .cache()

    val rdd_time_agg = rdd_step_1
        .map(item=>{
          val timeIterater = item._2.map(item => item._1)
          val listBuffer = BaseUtil.timeContinuousJudge(timeIterater,interval)
          (item._1,listBuffer)
        })
        .flatMapValues(x => x)

    val rdd_local_agg =rdd_step_1
        .map(item=>{
          val localIterter = item._2.map(item => BaseUtil.sliceLocalStr(item._2))
          val localBuffer = BaseUtil.geoRangeJudge(localIterter,geoRangeInvterval)
          (item._1,localBuffer)
        })
        .flatMapValues(x => x)

    val rdd_local_time =
      rdd_step_1
      .map(item=>{
        val iterator = item._2
        val tuples = BaseUtil.timeAndLocationContinuous(iterator)
        (item._1,tuples)
      })
      .flatMapValues(x => x)

    rdd_local_time
      .take(400)
      .foreach(println)
//
//    rdd_time_agg
//    .take(20)
//    .foreach(println)
//
//    rdd_local_agg
//      .take(20)
//      .foreach(println)

  }


  /**
    * 写入到MySQL数据库中
    * @param rdd
    * @param scheme
    */
  def write2MySQL(rdd:RDD[Row],scheme: StructType): Unit ={

  }

  def demoProcess():Unit ={
    sc.textFile(inputPath)
      .map(str => {
        val strList = StringUtil.fastSplit(str, "|")
        val phone: String = strList(0)
        (phone, str)
      })
      .reduceByKey((x, y) => {
        val x_str = StringUtil.fastSplit(x, "|")
        val x_1: Int = x_str(1).toInt
        val x_2: Long = x_str(2).toLong
        val x_3: String = x_str(3)

        val y_str = StringUtil.fastSplit(x, "|")
        val y_1: Int = y_str(1).toInt
        val y_2: Long = y_str(2).toLong
        val y_3: String = y_str(3)

        val ret_1 = x_1 + y_1
        val ret_2 = x_2 + y_2
        val ret_3 = x_3 + y_3
        s"${ret_1}|${ret_2}|${ret_3}"
      })
      .saveAsTextFile(outputPath)
  }


  def clusterProcess(sc:SparkContext,config: Config): Unit ={

  }

  def hbaseRead(): Unit ={
    val sparkSession = SparkSession.builder().appName("instrument").getOrCreate()

  }

  def demo_test_1()={
    val sparkSession = SparkSession.builder().appName("instrument").getOrCreate()
    val dfSeq = Seq(
      ("NAME1", "2019-03-22"),
      ("NAME1", "2019-03-23"),
      ("NAME1", "2019-03-24"),
      ("NAME1", "2019-03-25"),

      ("NAME1", "2019-03-27"),
      ("NAME1", "2019-03-28"),

      ("NAME2", "2019-03-27"),
      ("NAME2", "2019-03-28"),

      ("NAME2", "2019-03-30"),
      ("NAME2", "2019-03-31"),

      ("NAME2", "2019-04-04"),
      ("NAME2", "2019-04-05"),
      ("NAME2", "2019-04-06")
    )

    val dateFrame = sparkSession.createDataFrame(dfSeq)
      .toDF("stationName", "date")
      .withColumn("date", date_format(col("date"), "yyyy-MM-dd HH:mm:ss"))

    dateFrame.createTempView("stations");

    val result = sparkSession.sql(
      """
        |WITH s AS (
        |   SELECT
        |    stationName,
        |    date,
        |    date_add(date, -(row_number() over (partition by stationName order by date)) + 1) as discriminator
        |  FROM stations
        |)
        |SELECT
        |  stationName,
        |  MIN(date) as start,
        |  COUNT(1) AS duration
        |FROM s GROUP BY stationName, discriminator
      """.stripMargin)

    val result_1 = sparkSession.sql(
      """
        |SELECT
        |    stationName,
        |    date,
        |    date_add(date, -(row_number() over (partition by stationName order by date))+1 ) as discriminator
        |  FROM stations
      """.stripMargin)

    result.show()
  }

  def demo_test_2(dataFrame:DataFrame)={

    dataFrame.createTempView("work")
    val sparkSession = SparkSession.builder().appName("instrument").getOrCreate()

    val res = sparkSession.sql(
      """
        |with s AS(
        | SELECT  instrument_id,
        | report_time,
        | date_add(report_time, -
        |)
      """.stripMargin
    )


  }




}
