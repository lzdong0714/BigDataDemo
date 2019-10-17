package demo.process

import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats

import scala.util.parsing.json.JSON

class AnalyzerUtil extends java.io.Serializable {

  def parseItem(jsonStr:String): List[DataItem] ={
    implicit val formats = DefaultFormats
    import org.json4s._
    import org.json4s.native.JsonMethods._
    val  dataList = parse(jsonStr).extract[List[DataItem]]
    dataList
  }


  def writeCSVFile(rdd:RDD[Any],path:String,fileName:String)={

  }

}


case class DataItem(Key:String,Value:String,StandardKey:String,Type:String,Unit:String,Min:String,Max:String,
                    Priority:Int,Quality:String,Cycle:String,AvgIntervalValue:Long,AvgIntervalUnit:String,AvgTarget:String)