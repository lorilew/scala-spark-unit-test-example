package example

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

object SimpleApp extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val logFile = "example.txt"
    val logData = spark.read.textFile(logFile).cache()

    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
  }

  def makeAnimalDataframe(): DataFrame = {
    val someData = Seq(
      Row(8, "bat"),
      Row(64, "mouse"),
      Row(-27, "horse")
    )

    val someSchema = List(
      StructField("number", IntegerType, nullable = true),
      StructField("word", StringType, nullable = true)
    )

    spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      StructType(someSchema)
    )
  }

  def positiveAnimals(animalDF: DataFrame): DataFrame = {
    animalDF.createOrReplaceTempView("animalDF")
    spark.sql(s"select word from animalDF where number > 0")
  }
}

