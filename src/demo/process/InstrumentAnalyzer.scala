package demo.process

import java.sql.Timestamp

import demo.Submit.Config
import demo.schema.MeasureDataOutSchema
import demo.util.BaseUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class InstrumentAnalyzer(sc:SparkContext, config: Config) extends java.io.Serializable {

  def process(): Unit ={
    val  instrumentId = "QB91G3S01000100"
//    val dateFrameWork = instrumentWorkMySQL("LY50Q88888881")
    val dataFrameMeasure = instrumentMeasureMySQL(instrumentId)

    val rddStepOne = processStepOne(dataFrameMeasure)

    processStepTwo(dataFrameMeasure,rddStepOne)
  }

  //仪器数据处理 1 根据时间划分任务时段 2 提取任务时段的地址信息 3 根据任务时段获取数据分析
  def processStepOne(dataFrame: DataFrame): RDD[(String,(Timestamp,Timestamp,(Double,Double,Double)))] ={
    val taskRDD = divideBy(dataFrame)
    taskRDD.take(20).foreach(println)
    val workRDD = workTimeFilter(taskRDD,60)
    workRDD
  }

  def processStepTwo(dataFrame: DataFrame,rdd:RDD[(String,(Timestamp,Timestamp,(Double,Double,Double)))]): Unit ={
    val dataFrameList = getInfoByTimeWindow(rdd,instrumentMeasureTimeWindow)
    tmpFunc(dataFrameList)
  }

  def tmpFunc(dataFrameList:List[DataFrame]): Unit ={
    val analyzerUtil = new AnalyzerUtil
    var instrumentId:String = ""
    var reporterTime:Timestamp = null
    var longitude:Double = 0.0
    var latitude:Double = 0.0
    var altiude: Double = 0.0
    var dataItem:String = ""
//    var values:StringBuffer =new StringBuffer()
    val rddList = dataFrameList.map(dataFrame=>{
      val rdd=dataFrame.rdd
        .map(row=>{
          instrumentId = row.getAs[String](0)
          reporterTime = row.getAs[Timestamp](1)
          longitude = row.getAs[Double](4)
          latitude = row.getAs[Double](5)
          altiude = row.getAs[Double](6)
          dataItem = row.getAs[String](7)


          val items = analyzerUtil.parseItem(dataItem)
          (instrumentId,reporterTime,longitude,latitude,altiude,
            items)
        })
          .filter(tuple=>{
            tuple._6.size==8
          })
          .map(tuple=>{
            val items = tuple._6
            val valueList = items.map(dataItem=>{
              dataItem.Value
            })

            (instrumentId,reporterTime,longitude,latitude,altiude,
              valueList.head,valueList.apply(1),valueList.apply(2),valueList.apply(3),valueList.apply(4),
              valueList.apply(5),valueList.apply(6),valueList.apply(7))
          })
      rdd
    })

    var index:Int = 0
//    val path:String = "D:\\myproject\\bigdatademo\\demodata\\output"
    val path:String = s"D:\\workproject\\工程展示\\仪器分析\\data_result\\output"
    rddList.foreach(itemRdd=>{
      index = index+1
      val rowRdd=itemRdd.map(tuple=>{

        Row(tuple._1,tuple._2,tuple._3,tuple._4,tuple._5,tuple._6,tuple._7,tuple._8,tuple._9,tuple._10,
          tuple._11,tuple._12,tuple._13)
      })

      val data = sparkSession.createDataFrame(rowRdd,MeasureDataOutSchema.apply)
        data.write
//        .option("delimiter","|")
        .option("header","true")
        .format("com.databricks.spark.csv")
        .save(path+s"cvs_$index")

//          .write.csv(path+s"cvs_${index}")
//      itemRdd.saveAsTextFile(path+s"${index}")
    })
  }

  /**
    * 从MySQL获取原始数据，返回为DataFrame
    * @return
    */
    val sparkSession = SparkSession.builder().appName("instrument").getOrCreate()
    val jdbcDF = sparkSession.read.format("jdbc")
      .option("url","jdbc:mysql://47.92.159.163:3306/hnty?autoReconnect=true&" +
      "useUnicode=true&characterEncoding=utf-8&useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false" +
      "&serverTimezone=UTC&serverTimezone=Asia/Shanghai")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "MVC").option("password", "123456")

  // 工步数据表获取数据
  def instrumentWorkMySQL(instrumentID:String): DataFrame ={
    jdbcDF.option("dbtable", "work_history")//*****是表名
      .load()
      .createTempView("work_history_tmp")
    val stepID = "upload"
    val data_selected=sparkSession.sql(s"SELECT instrument_id, report_time,arrived_time,created_time,longitude,latitude,altitude" +
      s" FROM work_history_tmp WHERE instrument_id = '${instrumentID}' AND step_id = '${stepID}' ")

    data_selected
  }
  def instrumentWorkTimeWindow(instrumentID:String, startTime:Timestamp,endTime:Timestamp):DataFrame={
//    jdbcDF.option("dbtable", "work_history")//*****是表名
//      .load()
//      .createTempView("work_history_tmp")
    val stepID = "upload"
    val data_selected=sparkSession.sql(s"SELECT instrument_id, report_time,arrived_time,created_time,longitude,latitude,altitude" +
      s" FROM work_history_tmp WHERE instrument_id = '${instrumentID}' AND step_id = '${stepID}' AND report_time BETWEEN '${startTime}' AND '${endTime}'")
    data_selected
  }

//  非工步仪器的数据获取
  def instrumentMeasureMySQL(instrumentID:String):DataFrame = {
    jdbcDF.option("dbtable", "measure_history")//*****是表名
      .load()
      .createTempView("measure_history_tmp")
    val data_selected=sparkSession.sql(s"SELECT instrument_id, report_time,arrived_time,created_time," +
      s"longitude,latitude,altitude,data_json" +
      s" FROM measure_history_tmp WHERE instrument_id = '${instrumentID}' ")

    data_selected
  }

  def instrumentMeasureTimeWindow(instrumentID:String, startTime:Timestamp, endTime:Timestamp):DataFrame={
    val data_selected=sparkSession.sql(s"SELECT instrument_id, report_time,arrived_time,created_time," +
      s"longitude,latitude,altitude,data_json" +
      s" FROM measure_history_tmp WHERE instrument_id = '${instrumentID}' AND report_time BETWEEN '${startTime}' AND '${endTime}' ")
    data_selected
  }

  // 对工作时段划分
  def divideBy(dataFrame: DataFrame):RDD[(String,(Timestamp,Timestamp,(Double,Double,Double)))]={
    val interval:Long = 60*10 // 间隔时间（单位：秒）
    val geoRangeInvterval = 0.0001 // 地理经纬度间隔
    val data_afterFilter=dataFrame
    val rddRaw = data_afterFilter.rdd
    var instrumentId:String = ""
    var reporterTime:Timestamp = null
    var longitude:Double = 0.0
    var latitude:Double = 0.0
    var altiude: Double = 0.0
    var workTime:Long = 0
    val rdd_step_1 =
      rddRaw.map(row=>{
        instrumentId = row.getAs[String](0)
        reporterTime = row.getAs[Timestamp](1)
        longitude = row.getAs[Double](4)
        latitude = row.getAs[Double](5)
        altiude = row.getAs[Double](6)
        (instrumentId,(reporterTime,s"${longitude}|${latitude}|${altiude}"))
      })
        .groupByKey()
        .cache()


    //时间连续
//    val rdd_time_agg = rdd_step_1
//      .map(item=>{
//        val timeIterater = item._2.map(item => item._1)
//        val listBuffer = BaseUtil.timeContinuousJudge(timeIterater,interval)
//        (item._1,listBuffer)
//      })
//      .flatMapValues(x => x)
//
//
//    //空间连续
//    val rdd_local_agg = rdd_step_1
//      .map(item=>{
//        val localIterter = item._2.map(item => BaseUtil.sliceLocalStr(item._2))
//        val localBuffer = BaseUtil.geoRangeJudge(localIterter,geoRangeInvterval)
//        (item._1,localBuffer)
//      })
//      .flatMapValues(x => x)

    // 时间空间一致连续
    val rdd_local_time =
      rdd_step_1
        .map(item=>{
          val iterator = item._2
          val tuples = BaseUtil.timeAndLocationContinuous(iterator,interval)
          (item._1,tuples)
        })
        .flatMapValues(x => x)
    rdd_local_time
  }

  // 对连续有效的工作时长筛选(时间间隔单位：秒)
  def workTimeFilter(rdd:RDD[(String,(Timestamp,Timestamp,(Double,Double,Double)))],interval:Long):RDD[(String,(Timestamp,Timestamp,(Double,Double,Double)))]={
    val retRDD = rdd.filter(item=>{
      val startTime = item._2._1
      val endTime = item._2._2
      BaseUtil.isWorkTimeContinuous(startTime,endTime,interval)
    })

    retRDD
  }

  def getInfoByTimeWindow(rdd:RDD[(String,(Timestamp,Timestamp,(Double,Double,Double)))],f:(String,Timestamp,Timestamp)=>DataFrame):List[DataFrame]={
      val ret=
      rdd
      .collect()
      .toList
        //执行rdd 中的数据作为参数，但是rdd是一个transform的算子，数据没有序列化，所以先做action算子下的collect
        // 然后调用函数参数
      .map(item=>{
        println("start:|"+item._2._1+"|"+item._2._2)
        f(item._1,item._2._1,item._2._2)
      })
    ret
  }


  def myShow(rdd: RDD[(String)],num:Int)={
    print("------------rdd---------------")
    rdd
      .take(20)
      .foreach(println)
  }
}

/**
  * 出现“org.apache.spark.SparkException: Task not serializable"这个错误，一般是因为在map、filter等的参数使用了外部的变量，但是这个变量不能序列化。
  * 特别是当引用了某个类（经常是当前类）的成员函数或变量时，会导致这个类的所有成员（整个类）都需要支持序列化。解决这个问题最常用的方法有：
  * 如果可以，将依赖的变量放到map、filter等的参数内部定义。这样就可以使用不支持序列化的类；
  * 如果可以，将依赖的变量独立放到一个小的class中，让这个class支持序列化；这样做可以减少网络传输量，提高效率；
  * 如果可以，将被依赖的类中不能序列化的部分使用transient关键字修饰，告诉编译器它不需要序列化。
  * 将引用的类做成可序列化的
  */
