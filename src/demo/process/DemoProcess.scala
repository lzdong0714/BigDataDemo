package demo.process

import com.yunheit.common.util.StringUtil
import demo.Submit.Config
import demo.util.BaseUtil
import org.apache.spark.SparkContext

class DemoProcess(sc:SparkContext, config: Config) {
  val inputPath = BaseUtil.handle_OS_path(config.input)
  val outputPath = BaseUtil.handle_OS_path(config.output)

  def localProcess(): Unit = {
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
}
