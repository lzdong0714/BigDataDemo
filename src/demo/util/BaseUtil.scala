package demo.util

import java.sql.Timestamp

import com.yunheit.common.util.StringUtil
import scala.collection.mutable.ListBuffer

object BaseUtil{

  def handle_OS_path(path:String):String={
    //todo 区分系统路径
    path
  }

  def timeContinuousJudge(timeList:List[Timestamp],interval:Long):ListBuffer[Tuple2[Timestamp,Timestamp]]= {

    val retList = new ListBuffer[Tuple2[Timestamp, Timestamp]]

    var time_1: Timestamp = null
    var time_2: Timestamp = null
    //毫秒转化为秒
    val interval_second = interval*1000
    val orderedList = timeList.sortWith((x, y) => x.before(y))

    var time_0 = orderedList.head
    for (a <- 0 until orderedList.length - 1) {
      time_1 = orderedList(a)
      time_2 = orderedList(a + 1)
      if ((time_2.getTime - time_1.getTime) > interval_second) {
        retList += Tuple2.apply(time_0, time_1)
        time_0 = time_2
      }
    }
    if (retList.isEmpty) {
      retList += Tuple2.apply(time_0, time_0)
    }
    retList
  }

  /**
    * 判定工作时间的连续性
    * @param timeList 工作时间列表
    * @param interval 判定时间间隔
    * @return 连续工作的时间列表 List[workStartTime, workEndTime]
    */
  //根据时间聚合在一起
  def timeContinuousJudge(timeList:Iterable[Timestamp],interval:Long):ListBuffer[Tuple2[Timestamp,Timestamp]]={

    val retList = new ListBuffer[Tuple2[Timestamp,Timestamp]]

    var time_1:Timestamp=null
    var time_2:Timestamp=null

    val orderedList = timeList.toList.sortWith((x, y) => x.before(y))
    //毫秒转化为秒
    val interval_second = interval*1000
    var time_0 = orderedList.head
    for(a <-0 until  orderedList.length-2){
      time_1 = orderedList(a)
      time_2 = orderedList(a+1)
      if((time_2.getTime - time_1.getTime) > interval_second){
        retList+=Tuple2.apply(time_0,time_1)
        time_0 = time_2
      }
    }
    if(retList.isEmpty){
      retList+=Tuple2.apply(time_0,time_0)
    }
    retList
  }


  //对位置信息列表归类聚合，在一个范围内的聚合在一起
  def geoRangeJudge(localIterter: Iterable[(Double,Double,Double)], geoRangeInvterval: Double):
  ListBuffer[Tuple3[Double,Double,Double]] = {
    val localList = localIterter.toList
    val retList = new ListBuffer[Tuple3[Double,Double,Double]]
//    var local_show_1:Tuple3[Double,Double,Double] = null
//    var local_show_2:Tuple3[Double,Double,Double] = null
//    var local_show_0 = localItrter.head
    retList+=localIterter.head
    for(index <- 0 until localList.size-1){
      val isSameRange = isSameRangeByDegree(localList(index), localList(index+1), geoRangeInvterval)
      if(!isSameRange){
        retList+=localList(index)
      }
    }

    retList
  }

  // 经纬度判定是否在一个范围内
  def isSameRangeByDegree(local_first:Tuple3[Double,Double,Double],local_second:Tuple3[Double,Double,Double],geoRangeInvterval:Double):
  Boolean={

    val flag_1 = (local_first._1 - local_second._1)<geoRangeInvterval
    val flag_2 = (local_first._2 - local_second._2)<geoRangeInvterval
//    val flag_3 = (local_first._3 - local_second._3)<geoRangeInvterval

    flag_1^flag_2
  }

  def sliceLocalStr(str:String):Tuple3[Double,Double,Double]={
    if(str == null || str.equals(""))
      return null

    val strList = StringUtil.fastSplit(str, "|")

//    if (strList.length<3)
    val longitude:Double = strList.head.toDouble
    val latitude:Double = strList(1).toDouble
    val altitude:Double = strList(2).toDouble

    (longitude,latitude,altitude)
  }



  def timeAndLocationContinuous(itemIter:Iterable[(Timestamp,String)],interval:Long):
  ListBuffer[Tuple3[Timestamp,Timestamp,Tuple3[Double,Double,Double]]]={

//    val retList =new ListBuffer[Tuple3[Timestamp,Timestamp,Tuple3[Double,Double,Double]]]
    val retList =new ListBuffer[(Timestamp,Timestamp,(Double,Double,Double))]
    val itemList = itemIter.toList
                  .sortWith((x,y)=>x._1.before(y._1)).map(item=>{(item._1,sliceLocalStr(item._2))})

    var time_0 = itemList.head._1
    var time_1:Timestamp=null
    var time_2:Timestamp=null

    var local_1:Tuple3[Double,Double,Double]=null
    var local_2:Tuple3[Double,Double,Double]=null
    var local_0:Tuple3[Double,Double,Double]=itemList.head._2


    val interval_second = interval*1000
    val geoRangeInvterval = 0.0001

    for (index <- 0 until itemList.length-1){
      val item_first = itemList(index)
      val item_second = itemList(index + 1)
      time_1 = item_first._1
      time_2 = item_second._1

      if((time_2.getTime - time_1.getTime) > interval_second){
        retList += Tuple3.apply(time_0,time_1,local_0)
        time_0 = time_2
      }

      //  假定时间更换比范围更换更加细粒度
      local_1 = item_first._2
      local_2 = item_second._2
      if(!isSameRangeByDegree(local_1,local_2,geoRangeInvterval)){
        local_0 = local_1
      }

    }
    retList+=Tuple3.apply(time_0,time_2,local_0)
    retList
  }

  def isWorkTimeContinuous(startTime: Timestamp,endTime: Timestamp,interval:Long):Boolean={
    val time_diff = endTime.getTime - startTime.getTime
    //小于时间间隔，判定为连续数据
    Math.abs(time_diff/1000)>interval
  }


}
