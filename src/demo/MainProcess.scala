package demo
import process.{DemoProcess, InstrumentAnalyzer}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j
import org.slf4j.LoggerFactory

object MainProcess {

  val logger: slf4j.Logger = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {
    logger.info("start")
    System.setProperty("hadoop.home.dir", "D:\\myproject\\hadoop_exe\\")
    Submit.parser parse(args, Submit.Config()) match {
      case Some(config) =>
        operation(config)
      case _ => throw new IllegalArgumentException("Error parsing command line arguments.")
    }
  }

  def operation(config: Submit.Config): Unit =
  {

    val sparkConf: SparkConf = new SparkConf().setAppName(s"demo")

    if(config.model=="local") {
      logger.info("work in local model")
      sparkConf.setMaster("local[*]")
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val sc: SparkContext = new SparkContext(sparkConf)
      val manager = new DemoProcess(sc,config)
      manager.localProcess()
    }else if(config.model=="cluster") {
      logger.info("work in cluster model")
      val sc: SparkContext = new SparkContext(sparkConf)
      val manager = new DemoProcess(sc, config)
      manager.clusterProcess(sc,config)
    }else if(config.model == "instrument"){
      logger.info("work in local model")
      logger.info(s"work in ${config.model} analyzer")
      sparkConf.setMaster("local[*]")
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val sc: SparkContext = new SparkContext(sparkConf)
      val instrumentAnalyzer = new InstrumentAnalyzer(sc, config)
      instrumentAnalyzer.process()
    }
  }

}
