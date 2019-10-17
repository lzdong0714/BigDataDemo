import sbt.Keys.libraryDependencies
lazy val commonDep = Seq(

  //deduplicate with bdcsc2 on slf4j,so add provided
  libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21" % "provided",
  libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0" % "provided",

  //Spark
  libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "compile",
  libraryDependencies += "org.apache.spark" % "spark-sql_2.10"% "2.1.0" % "compile",
  libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "2.1.0" % "provided",
  libraryDependencies += "org.lz4" % "lz4-java" % "1.4.0",
  unmanagedJars in Compile += file("repository/betacatcommon.jar"),
//  unmanagedJars in Compile += file("repository/spark.jar"),
  libraryDependencies += "log4j" % "log4j" % "1.2.14" ,
  libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.2"% "provided" ,
  libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"
)

lazy val Demo = (project in file("src/demo"))
  .settings(commonDep: _*)
  .settings(
    scalaSource in Compile := baseDirectory.value,
//    libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.3.0",
    libraryDependencies += "log4j" % "log4j" % "1.2.14" ,
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.2"% "provided" ,
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.7",
    
    libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.11" ,

    // Assembly sbt ";Demo CarClue;clean;assembly"
    resourceDirectory in Compile := baseDirectory.value / "resource",
    target in assembly := baseDirectory.value,
    //打包时，排除scala类库
    //assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyJarName in assembly := s"../../model/Demo.jar"
  )

lazy val EasyRule = (project in file("src/easyrule"))
  .settings(commonDep: _*)
  .settings(
    scalaSource in Compile := baseDirectory.value,
    //    libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.3.0",
    libraryDependencies += "log4j" % "log4j" % "1.2.14" ,
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.2"% "provided" ,
    //    libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.11" ,
    libraryDependencies += "org.jeasy" % "easy-rules-core" % "3.2.0",
    libraryDependencies += "org.jeasy" % "easy-rules-mvel" % "3.2.0",
    libraryDependencies += "org.jeasy" % "easy-rules-support" % "3.2.0",
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.7",

    // Assembly sbt ";Demo CarClue;clean;assembly"
    resourceDirectory in Compile := baseDirectory.value / "resource",
    target in assembly := baseDirectory.value,
    //打包时，排除scala类库
    //assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyJarName in assembly := s"../../model/EasyRule.jar"
  )
