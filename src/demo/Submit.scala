package demo
object Submit {
  case class Config(
                     input:String ="",
                     hiveTable:String="",
                     dx_date:String="",
                     model:String="",
                     output:String="",
                     otherType:Int=0,
                     remain:String=""
                   )

  val parser=new scopt.OptionParser[Config]("DataFilter")
  {
    //对未解析参数不在报错
    override def errorOnUnknownArgument = false

    //数据路径
    opt[String]("input").optional.valueName("input").action((x, c) => c copy (input = x))
    //输出数据路径
    opt[String]("output").optional().valueName("output").action((x, c) => c copy (output=x))
    //Hive表名
    opt[String]("hiveTable").optional.valueName("hiveTable").action((x, c) => c copy (hiveTable = x))
    //本地或这集群模式，不填为集群模式，填"local"为本地模式
    opt[String]("model").optional().valueName("model").action((x, c) => c copy (model=x))
    //其他输入数据类型
    opt[Int]("otherType").optional().valueName("otherType").action((x, c) => c copy (otherType = x))
    //保留字段
    opt[String]("remain").optional().valueName("remain").action((x,c) => c copy (remain = x))
  }
}
