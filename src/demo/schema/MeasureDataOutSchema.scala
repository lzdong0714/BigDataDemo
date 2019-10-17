package demo.schema

import org.apache.spark.sql.types._

object MeasureDataOutSchema {
  // 2091 仪器的输出数据meta

  def apply: StructType = {
    val fields = Array(
      StructField("instrument_id",StringType,nullable = true),
      StructField("report_time",TimestampType,nullable = true),
      StructField("longitude",DoubleType,nullable = true),
      StructField("latitude",DoubleType,nullable = true),
      StructField("altitude",DoubleType,nullable = true),
      StructField("O3",StringType,nullable = true),
      StructField("Ba",StringType,nullable = true),
      StructField("Qs",StringType,nullable = true),
      StructField("T",StringType,nullable = true),
      StructField("Bat",StringType,nullable = true),
      StructField("Jy",StringType,nullable = true),
      StructField("Jt",StringType,nullable = true),
      StructField("Cs",StringType,nullable = true)
    )
    StructType(fields)
  }

}
