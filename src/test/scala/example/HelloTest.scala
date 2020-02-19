package example

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSpec


class HelloTest extends FunSpec
  with SparkSessionTestWrapper
  with DataFrameComparer {
    it("should return a dataframe with animals") {
      val actualDF = SimpleApp.makeAnimalDataframe()

      val expectedSchema = List(
        StructField("number", IntegerType, nullable = true),
        StructField("word", StringType, nullable = true)
      )

      val expectedData = Seq(
        Row(8, "bat"),
        Row(64, "mouse"),
        Row(-27, "horse")
      )

      val expectedDF =  spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("should return only bat and mouse") {
      val animalDF =  spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(8, "bat"),
          Row(64, "mouse"),
          Row(-27, "horse")
        )),
        StructType(List(
          StructField("number", IntegerType, nullable = true),
          StructField("word", StringType, nullable = true)
        ))
      )

      val actualDF = SimpleApp.positiveAnimals(animalDF)

      val expectedDF =  spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row("bat"),
          Row("mouse")
        )),
        StructType( List(
          StructField("word", StringType, nullable = true)
        ))
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }
}

